# WAL-recovery PMR resource mismatch: расследование SIGABRT в Debug-сборке

**Дата:** 2026-06-03
**Ветка:** `upgrade/actor-zeta-1.2.0` (HEAD = `c0d5185f`)
**Статус:** root cause установлен, минимальный фикс предложен (не применён)

## Симптом

Свежая Debug-сборка с нуля → 3 интеграционных теста падают по SIGABRT,
каждый крах убивает весь бинарь `test_otterbrix` (из ~87 кейсов исполняется ~19):

- `integration::cpp::test_wal_pool::update_wal_recovery`
- `integration::cpp::test_wal_pool::sql_dml_full_cycle`
- `integration::cpp::test_persistence::wal_recovery_dml_full_cycle`

```
Assertion failed: (resource_ == other.resource_),
function operator=, file validation.cpp, line 44.
```

Общий паттерн: фаза 2 теста — «restart and verify WAL replayed UPDATE».
Падает только реплей **UPDATE**; insert/delete-реплей проходит.

## Стек (lldb, `-k "bt"`)

```
base_spaces.cpp:233   WAL-replay лямбда → direct_update_sync(oid, row_ids, *r->physical_data)
  └ manager_disk_storage.cpp:122   manager_disk_t::direct_update_sync
    └ agent_disk.cpp:285           entry->storage->update(ids_vec, new_data)
      └ data_table.cpp:418         updates_slice.slice(data, ...)   ← updates_slice на resource_ агента
        └ vector.cpp:230/168/181   vector_t::slice → reference → reinterpret
          └ validation.cpp:44      validity_mask_t::operator= → assert(resource_ == other.resource_)
```

## Root cause

Конфликт **pmr-аллокаторов**, не конкурентность (всё в одном потоке main,
внутри конструктора `base_otterbrix_t` при реплее WAL):

- `r->physical_data` десериализован из WAL на **системном ресурсе** (`&resource`
  из `base_spaces.cpp`);
- `data_table_t` стораджа живёт на **ресурсе агента** (`agent->resource()`);
- `data_table_t::update` делает `updates_slice.slice(data, ...)` — zero-copy
  reference чужих векторов, и `validity_mask_t::operator=` требует совпадения
  ресурсов.

Почему insert-реплей не падает: `storage->append()` внутри **копирует** данные
(row_groups → сегменты колонок), а `update()` — **слайсит**. Падает ровно
комбинация «чужой ресурс × слайсящий update».

## Почему проявилось только сейчас («код не менялся, а падать начало»)

1. **Ассерт живёт только в Debug.** `assert(resource_ == other.resource_)`
   компилируется только без `NDEBUG`. Причём в NDEBUG-сборке этот код
   **функционально безопасен**: `operator=` всё равно делает deep-copy маски
   на *свой* `resource_` (validation.cpp:50,
   `std::make_shared<validity_data_t>(resource_, other.validity_mask_, count_)`).
   То есть несовпадение ресурсов — латентное, без ассерта оно никак себя
   не проявляет.

2. **Сборка с нуля сменила конфигурацию.** Старого build-каталога не было
   (конфигурировали заново), conan `--build=missing` в 22:31 собрал **первый
   Debug-бинарь actor-zeta** в кэше (`~/.conan2/p/b/actor55eb6382ee9f1`) — все
   прежние пакеты были только Release (gnu17). Это косвенно говорит, что прежние
   прогоны (включая «769/771 PASS» в сообщении HEAD-коммита `c0d5185f`,
   сделанного в тот же день в 22:12) шли с другим профилем сборки, где ассерты
   выключены.

3. **Сам mismatch старый** — появился с мега-коммитом `cad41ea9`
   (actor-zeta 1.2.0 + Pure MVCC): WAL-реплей десериализуется на системном
   ресурсе, а сторадж живёт на ресурсе агента (`direct_update_sync` как путь
   существует с «Disk for table» #462). Комбинация «Debug-ассерты +
   update-реплей» просто впервые исполнилась в этой конфигурации.

**Ограничение доказательства:** старый build-каталог удалён вместе со своим
`Testing/`-логом, поэтому профиль предыдущего зелёного прогона восстановлен
косвенно (по составу conan-кэша). Механизм при этом объясняется полностью:
изменилась конфигурация сборки, а не код.

## Проверенные и отброшенные гипотезы

| Гипотеза | Вердикт |
|---|---|
| Регрессия в последних коммитах (`c0d5185f`, `724ad7ff`) | Нет — ни один не трогает agent_disk/реплей/ресурсы векторов |
| Уехала зависимость actor-zeta («движущийся 1.2.0») | Нет — в кэше ровно одна recipe-ревизия 1.2.0 (от 2026-05-28); большой diff исходников оказался сравнением 1.1.1 ↔ 1.2.0 |
| Гонка между акторами | Нет — крах в одном потоке до старта шедулеров |

## Минимальный фикс (граница агента)

`agent_disk.cpp`, `direct_update_sync`, перед `storage->update()`:

```cpp
// new_data десериализован на ресурсе WAL-реплея; сторадж живёт на ресурсе
// агента. update() слайсит (zero-copy refs), поэтому материализуем локальную
// копию на resource() — то же граничное правило, что и ids_vec выше.
components::vector::data_chunk_t local(resource(), new_data.types(), new_data.size());
new_data.copy(local, 0);   // deep copy: vector_ops::copy по колонкам
entry->storage->update(ids_vec, local);
```

Почему это корректно (проверено по коду):

- `data_chunk_t::copy()` (data_chunk.cpp:172) — поэлементная копия через
  `vector_ops::copy`: validity через `copy_indexing` (бит-level `set` /
  `slice_in_place` на собственном буфере), строки пере-аллоцируются в
  string-буфер таргета на его ресурсе. Ресурсных ассертов на этом пути нет.
- Ктор `data_chunk_t(resource, types, capacity)` создаёт FLAT-вектора,
  `count_ = 0` → прекондишены `copy()` выполняются.
- `partial_copy()` **не подходит** — внутри он слайсит (zero-copy), вектора
  остаются на ресурсе источника (vector.cpp:61).
- `direct_append_sync` / `direct_delete_sync` менять не надо: append копирует
  внутри, delete строит `ids_vec` уже на `resource()`.

Цена: одна глубокая копия чанка на каждую UPDATE-запись WAL — только на пути
рекавери (bootstrap), горячий путь не затронут.

## Связь с async-first рефакторингом

Перевод реплея на mailbox/queue (см. `async-first-refactor.md`) **сам по себе
этот баг не чинит**: сообщение доносит тот же `data_chunk_t` с тем же
указателем на чужой аллокатор. Queue решает потоки и владение; материализация
на pmr-границе получателя нужна в обеих архитектурах — минимальный фикс
не выбрасывается, он становится частью контракта границы агента.

## Открытые хвосты

- Фикс не применён (ожидает решения).
- После фикса возможен следующий скрытый баг дальше по тем же тестам
  (рекавери-тесты падали на первом ассерте, дальнейший код не исполнялся) —
  критерий истины: пересборка + прогон `test_wal_pool::*` и `test_persistence::*`.
- Отдельно замечен вис фонового прогона полного сьюта (лог замер на DML
  insert, процесс пришлось убивать) — вероятная связь с известной
  кооперативной флэйковостью (см. async-first-refactor.md, «multithread bug»),
  требует отдельного воспроизведения.