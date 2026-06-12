# actor-zeta: находки и предложения по улучшению (по итогам TSAN-работ)

Статус-документ. Работы в actor-zeta не ведутся без отдельной команды владельца.
Версия на момент анализа: 1.2.0.

## 1. atomic_thread_fence не моделируется TSAN (главное предложение)

**Факт.** Shutdown-путь акторов и генераторы синхронизируются standalone-фенсами:
- `actor/cooperative_actor.hpp:573` — `std::atomic_thread_fence(seq_cst)` в
  `wait_for_resume_to_complete()` (вызывается из `~cooperative_actor()` и
  `begin_shutdown()`);
- `actor/cooperative_actor.hpp:597` — второй seq_cst-фенс того же пути;
- `detail/generator.hpp:81` — acquire-фенс.

ThreadSanitizer standalone-фенсы не моделирует ни в одной версии рантайма
(ограничение дизайна). gcc-12 добавил диагностику `-Wtsan`, помечающую каждую
инстанциацию; при сборке потребителя с `-Werror` это ломает TSAN-сборку.

**Текущий обход (на стороне otterstax CI).** `tsan-deps.profile` собирает
otterbrix и actor-zeta с `-Wno-error=tsan` — диагностика остаётся видимым
ворнингом.

**Влияние.** Корректность не затронута: happens-before на shutdown-пути
обеспечивают CAS-операции mailbox'а (seq_cst), которые TSAN моделирует; фенсы —
страховка поверх. Полный TSAN-прогон test_otterbrix с инструментированными
engine и actor-zeta чист. Но любой будущий ordering, держащийся ТОЛЬКО на этих
фенсах, для TSAN невидим.

**Предложение.** Заменить standalone-фенсы парными атомарными операциями на
реальной переменной (release-store / acquire-load; для seq_cst-пар — RMW на
общем атомике). Эквивалентно по memory model, полностью моделируется TSAN,
устраняет -Wtsan; после этого `-Wno-error=tsan` удаляется из профиля otterstax.

## 2. Подтверждённое здоровье (проверено, менять не нужно)

- **unique_future / shared_state** (`detail/future.hpp`,
  `detail/shared_state.hpp`): синхронизация корректна — `set_value` пишет
  значение и поднимает флаг `fetch_or(value_set, release)`, чтения флагов
  `acquire`, continuation через `acq_rel` exchange/CAS, финализация — CAS
  `acq_rel`. Подозрения на «дыру в final_suspend» не подтвердились.
- **default_mailbox / lifo_inbox**: push/pop на seq_cst CAS
  (`lifo_inbox.hpp:32/:81`) — корректный hand-off. ВАЖНО: реализация лежит
  out-of-line в скомпилированной части пакета (`src.cpp` через src.hpp), поэтому
  TSAN-потребители ОБЯЗАНЫ пересобирать actor-zeta с `-fsanitize=thread`, иначе
  все передачи значений между акторами выглядят гонками (наблюдалось: 144
  ложных ворнинга), а настоящие гонки маскируются.

## 3. Предложения по упаковке/диагностике

- Рассмотреть публикацию TSAN-флейвора conan-пакета (или опции рецепта),
  чтобы потребителям не требовался `--build="actor-zeta/*"` с ручными флагами.
- gcc-11 libtsan лишён перехватчика `pthread_cond_clockwait`
  (google/sanitizers#1259, GCC PR101978) → ложные «double lock of a mutex» на
  каждом `condition_variable::wait_for` под gcc-11; в документации/CI-рецептах
  потребителей фиксировать минимум gcc-12 для TSAN.

## 4. Мелочь у потребителя (otterbrix), связанная с actor-zeta-паттернами

`services/wal/manager_wal_replicate.cpp:171`: `lock_guard` вокруг
`notify_one()` в деструкторе не закрывает окно пропущенного notify (loop
проверяет флаг вне мьютекса; `wait_for` ограничен 100 мкс) — лок можно удалить.
Записано также в backlog-development.md (ядро, п.4).