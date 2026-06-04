# actor-zeta: lost-wakeup в cooperative_actor — диагноз и доработки

**Дата:** 2026-06-04
**Статус:** корень доказан инспекцией живого процесса; фикс предложен (не применён — решение за автором библиотеки)
**Симптом в otterbrix:** вис `test_otterbrix_multithread` (конкурентные INSERT, 4 потока) — известный флэйк; re-enqueue из `production_idle_tick` не помогает даже на 100k/сек (зафиксировано ещё в b54f3256).

## Доказательная база (живой процесс, lldb)

Вис воспроизведён, у зависшего процесса сняты стеки всех потоков и состояние акторов:

| Наблюдение | Значение |
|---|---|
| executor[0] `current_behavior_.is_busy()` | **true** (корутина suspended) |
| executor[0] `is_awaited_ready()` | **true** (future давно готов!) |
| `awaited_continuation_` slot | **non-null** (0x5e403a810 — continuation на месте) |
| `awaited_flags_` | 9 = value_set \| promise_released |
| executor[0] `state_` | **idle** (0) |
| executor[0] `mailbox().blocked()` | **true** ← ключ |
| `resume_impl<executor_t>` | вызывается постоянно (брейкпоинт бьётся на 3 воркерах) |
| CPU | 72% (горячий no-op цикл), RSS — плоский |
| Прогресс | ноль |

**Контрольный эксперимент:** ручной `try_unblock()` mailbox'а через lldb → система мгновенно сдвинулась (в логе появились `execute_plan: result received` спустя 8 часов виса), затем сработал ассерт инварианта `lifo_inbox::take_head:78` (ожидаемо — вмешательство мимо протокола). Корень подтверждён.

## Механизм

```
1. Корутина executor'а suspended'ится на cross-actor co_await.
   В окне суспензии awaited_flags_ ещё не опубликованы → is_busy() == false.
2. resume_impl в этом окне: «не busy, mailbox пуст» → try_block() → актор ПАРКУЕТСЯ
   (mailbox → reader_blocked).
3. Producer завершает future: release_promise ставит ТОЛЬКО флаг promise_released.
   Mailbox никто не разблокирует (готовность — не сообщение).
4. Каждый последующий resume (хоть 100k/сек от re-enqueue):
       cooperative_actor.hpp:292   if (mailbox().blocked())
                                       return awaiting;        ← СРАБАТЫВАЕТ ПЕРВОЙ
       cooperative_actor.hpp:299   if (current_behavior_.is_busy()) { Q6 ... }  ← НЕ ДОСТИГАЕТСЯ
5. Готовый continuation никогда не берётся. Вечный no-op цикл.
```

Поэтому re-enqueue-заплатка (`production_idle_tick` диспетчера) бессильна: проблема не в «актора забыли запланировать», а в том, что resume **отваливается на blocked-чеке до Q6**.

## Предлагаемый фикс (cooperative_actor.hpp, entry resume_impl)

Поднять Q6-блок выше blocked-чека + re-check после resume (зеркально in-loop guard'у):

```cpp
// Q6 FIRST — обязан исполняться даже при заблокированном mailbox (parked):
// готовность awaited-future флаговая (release_promise), inbox она не разблокирует.
if (current_behavior_.is_busy()) {
    if (current_behavior_.is_awaited_ready()) {
        auto cont = current_behavior_.take_awaited_continuation();
        if (cont) {
            cont.resume();
        }
    }
    // Re-check (зеркало in-loop guard'а): await мог быть не готов, либо
    // корутина после resume пере-suspended'илась на следующем co_await.
    // Падение отсюда в blocked-return / try_block re-strand'ит её снова.
    if (current_behavior_.is_busy()) {
        return finalize(scheduler::resume_result::resume, 0, true);
    }
}

if (mailbox().blocked()) {
    return finalize(scheduler::resume_result::awaiting, 0, false);
}
```

Свойства:
- **Самоизлечение**: даже если гонка из шага 1-2 запаркует актора, первый же resume (от re-enqueue / нового сообщения) дренирует готовый continuation. Гонку публикации awaited_flags_ можно чинить отдельно — система перестаёт быть к ней фатально чувствительной.
- Заодно закрывается вторая асимметрия entry-пути: «busy+ready → resume → re-suspend → провал в try_block → drop» (in-loop путь имеет guard на :348, entry — не имел).
- После этого фикса re-enqueue-заплатка в otterbrix (`production_idle_tick` диспетчера: цикл по executors_ + 10µs sleep) становится удаляемой.

## Сопутствующие доработки (кандидаты, по результатам анализа)

1. **Публикация awaited_flags_ vs try_block** — первопричина паркования busy-актора: закрыть окно (порядок публикации цепочки до возврата управления из суспензии), либо считать допустимой при наличии самоизлечения выше.
2. **`lifo_inbox::take_head:78` инвариант** — ассерт корректен; внешних вмешательств не предполагает. Не трогать.
3. **Документировать контракт**: «готовность future не будит актора — будит только push в mailbox или принудительный resume; resume обязан проверять busy/ready до любых early-return'ов».

## Воспроизведение / верификация фикса

- Репро: `test_otterbrix` `integration::cpp::test_otterbrix_multithread` (4×INSERT по 25k значений) — висит с вероятностью ~50-100% на прогон до фикса.
- Критерий фикса: 10 прогонов подряд без виса + существующие тесты actor-zeta зелёные.
- Минимальный standalone-репро для test-suite библиотеки: актор A co_await'ит актора B; между суспензией и публикацией awaited-цепочки врезать yield (или гонять под нагрузкой 4+ потоков отправителей); проверить, что A доезжает после готовности B при предварительно заблокированном mailbox A.