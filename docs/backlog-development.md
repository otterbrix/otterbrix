# Otterbrix: полный бэклог доработок

Статус-документ. Здесь зафиксировано ВСЁ по otterbrix: что сделано, что готово
к доделке (с принятыми дизайн-решениями), что отложено. Работы не ведутся без
отдельной команды владельца.

## A. Сделано (в main через PR #515 + тег 1.0.0a13-rc-3)

- Фиксы validity_mask_t (resource в ptr-конструкторе; 64× over-read
  `count_` vs `entry_count`), чистка null_memory_resource из агрегатного пути,
  gather-once в operator_group.
- MVCC: watermark-gated `data_table_t::compact(uint64_t)` —
  `transaction_manager_t::compact_watermark()` едет в сообщениях, решающая
  проверка локальна (`has_version_above`); checkpoint скипает несжимаемые
  таблицы; abort implicit-транзакций на error-курсоре (утечка прижимала
  watermark).
- Lock-free eviction queue буфер-пула (boost::lockfree, pmr-узлы) — была
  гонка: голый `std::queue::push` без лока с конкурентных unpin-путей.
- `session_id_t`: счётчик из результата `fetch_add` (были дубли id из
  конкурентных потоков → две операции делили одну транзакцию).
- wrapper_dispatcher схлопнут до одного канала `execute_plan`
  (11 интерфейсных методов удалены, колл-сайты на логических планах).
- Тесты: `[engine-lifecycle]` (4 кейса: владение через use_count, конкурентный
  eviction-шторм), MVCC-репро compact'а, session-uniqueness.

## B. Готовая незалитая работа: ветка `fix/scan-string-view-lifetime` (2 коммита запушены)

- `fd772acd` test(mvcc): scanned strings must survive compact (репро 5/5).
- `65e8459c` fix(table): `string_scan_partial`/`string_fetch_row` материализуют
  строки в буфер result-вектора (идиом `impl::own_string`); + фикс UAF в самом
  test_returning (string_view от временного logical_value_t).
- Верифицировано: test_table 59/13.7M, test_otterbrix 51439/206 (бейзлайн main).
- PR не открыт.

## C. Починочное ядро (баги; дизайн готов, работа не начата)

1. **update_segment string-фетчи** (`components/table/update_segment.hpp`):
   `update_merge_fetch<string_view>`, `templated_fetch_committed{,_range}`,
   `templated_fetch_row` пишут view в heap_ сегмента, умирающий при
   compact-swap. Фикс: протащить result-`vector_t&` до точек записи,
   материализовать идиомом `own_string`. Тест: UPDATE-вариант
   `scanned_strings_survive_compact`.
2. **fixed_size_scan zero-copy** (`components/table/column_segment.cpp` ~:478,
   `result.set_data(block_ptr)`): РЕШЕНИЕ — пин в векторе: расширить
   aux-механизм `vector_buffer_t` типом, удерживающим `buffer_handle_t`;
   zero-copy сохраняется, блок жив пока жив вектор. Тест: full-scan →
   удержать чанк → eviction/compact → assert значений. Если пины от
   долгоживущих курсоров надавят на буфер-пул — материализация на границе
   курсора.
3. **count_valid ragged-tail** (`components/vector/validation.cpp`): в
   последнем 64-битном слове не маскируются padding-биты при count % 64 != 0 —
   MAX_ENTRY-слово завышает счётчик. Фикс: маскирование как в
   `slice_in_place()`. Тест в test_validity_mask. Проверить потребителей
   (статистика null, кардинальности).
4. **WAL dtor lock** (`services/wal/manager_wal_replicate.cpp:171`): удалить
   `lock_guard` вокруг notify_one — окно пропущенного notify он не закрывает
   (loop читает флаг вне мьютекса), `wait_for` ограничен 100 мкс. Однострочник.
5. **Sublink-обрыв → явная ошибка**
   (`components/sql/transform/transfrom_common.cpp:597–653`): молча
   отбрасываемые формы саблинков (EXPR/ARRAY/CTE, часть ANY/ALL) теряют
   предикат → НЕВЕРНЫЙ результат запроса. Фикс: error_t «sublink form not
   supported» + негативные тесты. Без имплементации semi-join.

## D. Развитие (отложено; решения приняты)

1. **Нативные саблинки**: IN(SELECT)→semi-join (SEMI/ANTI-виды в node_join_t,
   физика на node_hash_join_t: probe выдаёт строку максимум раз, build не в
   выходе; примитивы: types::hash_row, vector_ops::hash); скалярный
   EXPR-саблинк → под-план первым в sequence + параметр. РЕШЕНИЕ: NULL-семантика
   NOT IN — стандарт SQL (NULL в списке ⇒ UNKNOWN ⇒ строка отбрасывается);
   anti-join null-aware; тесты-спеки до имплементации.
2. **Columnar-группировка + горячий logical_value_t** (один код): fallback
   operator_group.cpp:477–503 боксирует ключи per-row; finalize агрегатов
   (compute/kernels/aggregate.cpp:32–161) и value_internal()
   (vector.cpp:583–727) — топ горячих мест (~256 вхождений). РЕШЕНИЕ: дизайн
   «типизированные колонки ключей» — хеш-таблица хранит индекс группы, ключи
   лежат мини-таблицей vector_t-колонок (NULL через validity, строки через
   string_vector_buffer_t, финализация почти бесплатна); хеш векторный;
   сравнение — типизированный compare по колонкам. Отвергнуто (зафиксировано):
   сырая строка ключей + арена (DuckDB row layout) — быстрее memcmp, но новая
   подсистема раскладки; миграция локальна при нужде. Волны: (1) расширить
   group-by тесты ДО переработки; (2) ключи, жёсткая замена без feature-flag;
   (3) finalize в типизированные слоты + бенчмарк до/после.
3. **Остаток зачистки logical_value_t** (~950 продовых вхождений из ~1205;
   тестовые ~879 не трогаются): ПОСЛЕ columnar-группировки (она даёт образец).
   Порядок: index codec (~72, несущая type erasure — дизайн полиморфной
   сериализации) → cursor API (~9) → SQL-транзформер (~58) → холодный хвост.
4. **Фенсы actor-zeta** — см. `docs/actor-zeta-improvements.md`.

## E. Известные ограничения/заметки

- TSAN-прогоны движка имеют смысл только при инструментированном actor-zeta
  (см. actor-zeta-doc); один benign-паттерн boost::lockfree остаётся
  (suppression `race:boost::lockfree`).
- gcc-12 для TSAN-сборок обязателен (gcc-11 libtsan без перехватчика
  pthread_cond_clockwait → ложные «double lock» на каждом cv::wait_for).