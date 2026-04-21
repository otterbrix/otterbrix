# Оставшиеся склейки `data_chunk_t` и как их убрать

## Контекст

Инвариант проекта после миграции на multi-chunk: внутри пайплайна не должно возникать одного
`data_chunk_t` размером больше `DEFAULT_VECTOR_CAPACITY = 1024`. Все `operator_data_t` должны
нести `std::vector<data_chunk_t>`, каждый элемент ≤ 1024 строк.

Ниже — места, где сейчас этот инвариант нарушается (в момент выполнения создаётся временный
chunk произвольного размера).

---

## Где ещё есть склейки

### 1. ~~`components/physical_plan/operators/operator_sort.cpp`~~ — ✅ DONE

Переделано на streaming k-way merge:

- **Phase 1**: каждый входной чанк сортируется локально через `std::sort` по `indexing_vector_t`
  с `columnar_sorter_t::operator()` (кэширует `k.vec` через `set_chunk`).
- **Phase 2**: k-way min-heap merge. Cursor = `{chunk_idx, cursor_in_sorted_indices}`.
  Сравнение через новый `columnar_sorter_t::compare_cross(chunk_a, row_a, chunk_b, row_b)` —
  резолвит `col_path` на каждую сторону, не использует кэшированные указатели.
- **Phase 3**: offset/limit применяется по ходу мерджа (пропускаем первые offset, стопим на take).
- Выход: `std::vector<data_chunk_t>`, каждый ≤ 1024 строк — копирование построчно через
  `vector_ops::copy(src, dst, row+1, row, cur_filled)`.

Пик по памяти: O(N чанков × sorted_indices по 4 байта на строку) + один выходной chunk.
Никакого склеивания входа/выхода.

### 2. ~~`components/physical_plan/operators/operator_group.cpp`~~ — ✅ input concat удалён

Склейки входа больше нет. Принятая схема (гибрид, см. обсуждение):

1. Вычисление computed-keys — по каждому входному чанку отдельно (в конце удаляем).
2. `create_list_rows` итерирует `left_->output()->chunks()`. В `row_refs_per_group_` лежат
   пары `(chunk_idx, row_idx)` (4+4 байта), а не плоские row-id.
3. Vectorized aggregation: считаем `group_ids_per_chunk[ci][row]`, для каждого агрегата
   вызываем `aggregate::update_all` по каждому чанку, `states` накапливаются across chunks.
4. Fallback (UDF-агрегаты) — per-group gather из multi-source: ищем runs подряд идущих
   строк одного source chunk и копируем их одним `vector_ops::copy` (близко к flat-memcpy).
5. Build result + post-aggregates + HAVING — пока как раньше, потом `split_large_output`.

Остающийся пик памяти: `2× num_groups` кратко (на build result + split). Это **не** зависит
от размера входа, только от числа уникальных групп. Полный streaming output — следующий шаг.

### 3. ~~`components/physical_plan/operators/operator_join.cpp`~~ — ✅ DONE

Nested-loop streaming join. Обе стороны итерируются через `left_->output()->chunks()` /
`right_->output()->chunks()` — никакого `data_chunk()` / concat.

- Схема джойна (`res_types`, `indices_left_`, `indices_right_`) строится один раз из
  `left_chunks.front().types()` / `right_chunks.front().types()` — все чанки одной стороны
  разделяют схему.
- Predicate создаётся один раз из этих схем и переиспользуется на каждую пару
  `(L_chunk, R_chunk)` через `batch_check_1vN(pred, L, R, li, R.size())` (inner/full/left)
  или `batch_check_Nv1` (right).
- Стриминг-выход через локальный `join_builder`: владеет `cur_` (текущий chunk ≤ 1024)
  и `out_chunks_`; `emit_matched` / `emit_left_only` / `emit_right_only` копируют одну
  строку через `vector_ops::copy(src, dst, idx+1, idx, filled_)` и флашят на `DEFAULT_VECTOR_CAPACITY`.
- FULL: `visited_right[ci_r][rj]` bitset по R чанкам, непосещённые эмитятся в конце.
- LEFT / RIGHT: flag `any_match` на текущей строке L (resp. R), при отсутствии — emit_left_only
  (resp. emit_right_only).
- CROSS: двойной цикл без predicate.

Пик памяти: O(|R|) bitset для FULL + один выходной chunk. Никаких склеек входа/выхода.

Квадратичная работа (|L| × |R|) остаётся — для equi-join будущий `operator_hash_join_t`
снизит до O(|L| + |R|), см. раздел 3.3 ниже.

### 4. ~~`services/collection/executor.cpp` — index-mirror в DELETE/UPDATE~~ — ✅ DONE

Контракт `manager_index_t::{insert,delete,update}_rows` переделан на
`std::vector<data_chunk_t>`. Реализация внутри использует пары cursor'ов
`(chunk_idx, row_idx_in_chunk)` и глобальный индекс для `row_ids` / `start_row_id`.
Executor во всех call-site'ах (INSERT/DELETE/UPDATE mirror + CREATE INDEX backfill)
итерирует `scan_out->chunks()` / `waiting_op->output()->chunks()` и строит
`std::vector<data_chunk_t>` через `partial_copy` на каждый chunk.

Склейка через `.data_chunk()` убрана во всех этих путях.

### 5. ~~`services/disk/manager_disk.cpp:storage_scan_segment`~~ — ✅ DONE

Сигнатура изменена на `unique_future<std::vector<data_chunk_t>>`. Реализация
просто пушит `chunk.partial_copy(resource, 0, chunk.size())` в вектор per callback —
никакой склейки. То же для `manager_disk_empty_t::storage_scan_segment`. Call-sites
в dispatcher (REBUILD indexes) и executor (CREATE INDEX backfill) передают этот вектор
напрямую в `manager_index_t::insert_rows`, считая `count` как сумму размеров чанков.

---

## Как убрать склейки в sort / group / join

Для всех трёх операторов общая стратегия — не читать `data_chunk()`, а итерировать
`left_->output()->chunks()`. Выход сразу писать как `std::vector<data_chunk_t>` в
`append_chunk(...)`. Детали зависят от оператора.

### 3.1. `operator_sort`

#### Вариант A: external merge sort (самый честный)

1. Для каждого входного chunk строится локальный `indexing_vector_t` и chunk сортируется
   независимо (chunk already ≤ 1024 — помещается в кэш, `std::sort` идеален).
2. Получаем `N` отсортированных чанков.
3. K-way merge через min-heap: в кучу кладём (ключ, chunk_idx, row_idx); на каждый шаг
   забираем минимум, пишем в текущий выходной chunk, продвигаем курсор в источнике.
4. Выходной chunk наполняется до 1024, затем `output_->append_chunk(...)` и новый пустой.

Память: O(N) курсоров + выходной chunk. Скорость: O(total · log N).

Плюсы: полностью стриминг на выходе. Минусы: для небольших входов медленнее текущего
одно-chunk `std::sort` из-за heap-overhead.

#### Вариант B: sort-by-index без склейки данных

1. Построить глобальный массив `{ chunk_idx, row_idx, key_value }` — по одному элементу на
   строку. Размер O(total), но это плоский массив, а не чанк данных.
2. `std::sort` по ключу.
3. Пройти отсортированный массив, писать строки в выходные чанки порциями по 1024, гейтер-копия
   из нужного `chunk_idx` с `indexing_vector_t`.

Память: O(total) индексов (8–16 байт на строку) + выход. Отлично работает для LIMIT: можно
partial_sort первые K индексов и построить один выходной chunk.

#### Вариант C: top-K heap (только если `limit_ >= 0`)

1. Держим min-heap размера K (`limit_.limit()`).
2. На каждый входной chunk: для каждой строки — если heap не полный, пуш; иначе сравнение
   с вершиной, возможно replace.
3. В конце — вычерпать heap, развернуть, разложить по чанкам.

Память: O(K). Скорость: O(total · log K). Идеально для частого случая `ORDER BY x LIMIT N`.

**Рекомендация**: C для `limit >= 0 && limit <= 10000`, иначе A.

### 3.2. `operator_group`

Классический путь — **hash aggregation one-pass**:

1. Завести `std::pmr::unordered_map<hash, group_state>` — по одному `group_state` на уникальный
   ключ. `group_state` хранит: копию значений ключа + аккумуляторы агрегатов (`raw_agg_state_t`).
2. Итерировать `left_->output()->chunks()`. Для каждого chunk:
   - Батч-хэш всех строк (уже делается сейчас через `chunk.hash(col_ids, hash_vec)`).
   - Для каждой строки: найти или создать группу, вызвать `update_all(kind, vec, group_ids, ...)`
     для каждого агрегата (тут `vec` — колонка текущего chunk, а `group_ids` указывают, в какую
     группу писать каждую строку **в рамках этого chunk**).
3. После последнего chunk — финализировать состояния в `logical_value_t` (как сейчас).
4. Построить выход: итерировать группы порциями по 1024, в каждом выходном chunk заполнить
   колонки ключей и агрегатов. `append_chunk` в `operator_data_t`.

Сложности:
- `row_ids_per_group_` (сейчас хранит, какие **строки склеенного chunk** принадлежат группе)
  заменяется на аккумуляторы. Нужен fallback путь для UDF-агрегатов, которые требуют видеть
  собранный subchunk группы — для них можно сохранять `std::vector<(chunk_idx, row_idx)>` вместо
  глобальных индексов, и на финализации гейтерить по этой «ссылке».
- `HAVING`-фильтр работает уже на построенном результате — остаётся без изменений, просто
  фильтрует в пределах одного выходного chunk или переходит к следующему.
- `post_aggregates` тоже считаются на выходных чанках после построения.

Память: O(num_groups × (ключ + агрегат_state)). Это **неизбежно** — это характеристика
GROUP BY. Но это **не склейка входа**.

### 3.3. `operator_join`

#### Hash join (для equi-join — самый важный случай)

1. Определить build-сторону (обычно меньшая). Пусть это `right`.
2. Собрать хэш-таблицу: `std::pmr::unordered_multimap<hash, (chunk_idx, row_idx)>` — вход
   итерируется через `right_->output()->chunks()`, ничего не склеивается.
3. Probe: итерировать `left_->output()->chunks()`. Для каждой строки левого — hash lookup,
   для каждого попадания — применить полный predicate (хэш может ошибаться на collision),
   и если проходит — emit строку в текущий выходной chunk.
4. Заполнив выходной chunk до 1024, `append_chunk` и новый пустой.

Для outer joins нужен bitset «какие строки build-стороны посещены», чтобы в конце добавить
непосещённые с NULL на probe-колонках.

Плюсы: O((|L| + |R|) × cost_probe), никаких склеек. Минусы: не работает для произвольных
predicate (только equi). Сейчас `operator_join` делает **nested-loop** с batch-predicate —
hash join нужен отдельный оператор (планировщик выбирает).

#### Nested-loop join без склеек

Сохранить текущую логику, но:
1. Итерировать внешний цикл по `left_chunks`.
2. Для каждой левой строки — вложенный цикл по `right_chunks`.
3. Результаты складывать в accumulator. Когда accumulator достигает 1024 — `append_chunk`
   и новый пустой.
4. Для outer joins: `visited_right` — bitset по всем правым chunks (индексация
   `(chunk_idx, row_idx)` → флаг).

Минус — квадратичная работа не меняется, но хотя бы убираем склейки.

#### Cross join

Аналогично nested-loop, только без predicate. Размер результата = |L| × |R|, поэтому
**обязательно** резать по 1024 — иначе потенциально гигантский chunk.

### 3.4. Index-mirror в `executor.cpp` (пункт 4 выше)

Две опции:

**A. Изменить контракт `manager_index_t::insert_rows` / `delete_rows` / `update_rows`**
принимать `std::vector<std::unique_ptr<data_chunk_t>>` вместо одного chunk. Внутри
итерировать по чанкам, поддерживая глобальный row-offset для `start_row_id`.

**B. Оставить контракт, но в executor итерировать `scan_out->chunks()`** и слать индекс-менеджеру
отдельное сообщение на каждый chunk. Более инвазивно для actor-пайплайна (больше message-round-trip).

Рекомендация — A.

### 3.5. `storage_scan_segment` / backfill индексов (пункт 5)

Та же развилка. Если контракт `insert_rows` меняется на vector-of-chunks, то
`storage_scan_segment` тоже меняет возвращаемый тип с `unique_ptr<data_chunk_t>` на
`std::vector<data_chunk_t>` — тогда callback просто пушит чанки в вектор, никаких копий.

---

## Порядок работ (минимально инвазивный)

1. **operator_join → hash join** (новый оператор `operator_hash_join_t`, планировщик выбирает
   для equi-conditions). Текущий nested-loop остаётся fallback.
2. **operator_group → streaming hash agg** (one-pass по `chunks()`), fallback на текущий путь
   только для UDF-aggregates, требующих subchunk.
3. **operator_sort → варианты A+C** (external merge + top-K для LIMIT-случая).
4. **Изменить `manager_index_t::*_rows` контракты** на приём `std::vector<data_chunk_t>` и
   убрать склейки в executor + `storage_scan_segment`.

Каждый шаг — отдельный PR, тесты `integration::cpp::test_batch_boundaries` с `row_count > 1024`
покрывают регрессии.
