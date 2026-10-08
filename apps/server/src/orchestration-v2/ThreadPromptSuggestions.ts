import type { ThreadId, ThreadPromptSuggestion } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

/**
 * The latest next-prompt suggestion per thread, for the composer's ghost text.
 *
 * Memory only. Adapters set it when a turn settles and clear it when the next
 * turn starts or the session ends; a restart forgets it. The default reference
 * drops writes, keeping adapter construction dependency-free in tests; the
 * live layer must be the one shared by the adapter infrastructure and the
 * WebSocket handlers so layer memoization yields one store.
 */
export interface ThreadPromptSuggestionsShape {
  readonly set: (threadId: ThreadId, text: string) => Effect.Effect<void>;
  readonly clear: (threadId: ThreadId) => Effect.Effect<void>;
  /** Emits the current suggestion (or null) first, then every change until unsubscribed. */
  readonly stream: (threadId: ThreadId) => Stream.Stream<ThreadPromptSuggestion | null>;
}

export class ThreadPromptSuggestions extends Context.Reference<ThreadPromptSuggestionsShape>(
  "t3/orchestration-v2/ThreadPromptSuggestions",
  {
    defaultValue: () => ({
      set: () => Effect.void,
      clear: () => Effect.void,
      stream: () => Stream.make(null),
    }),
  },
) {}

export const make = Effect.gen(function* () {
  const suggestions = yield* Ref.make(new Map<ThreadId, ThreadPromptSuggestion>());
  const changes = yield* PubSub.unbounded<{
    readonly threadId: ThreadId;
    readonly suggestion: ThreadPromptSuggestion | null;
  }>();

  const set = (threadId: ThreadId, text: string) => {
    const trimmed = text.trim();
    if (trimmed.length === 0) return clear(threadId);
    const suggestion: ThreadPromptSuggestion = { threadId, text: trimmed };
    return Ref.update(suggestions, (current) => new Map(current).set(threadId, suggestion)).pipe(
      Effect.andThen(PubSub.publish(changes, { threadId, suggestion })),
      Effect.asVoid,
    );
  };

  const clear = (threadId: ThreadId): Effect.Effect<void> =>
    Ref.modify(suggestions, (current) => {
      if (!current.has(threadId)) return [false, current] as const;
      const next = new Map(current);
      next.delete(threadId);
      return [true, next] as const;
    }).pipe(
      Effect.flatMap((removed) =>
        removed ? PubSub.publish(changes, { threadId, suggestion: null }) : Effect.void,
      ),
      Effect.asVoid,
    );

  // One-slot sliding mailbox per subscriber: only the newest value matters.
  const stream = (threadId: ThreadId) =>
    Stream.callback<ThreadPromptSuggestion | null>(
      (mailbox) =>
        Effect.gen(function* () {
          const subscription = yield* PubSub.subscribe(changes);
          const initial = (yield* Ref.get(suggestions)).get(threadId) ?? null;
          Queue.offerUnsafe(mailbox, initial);
          yield* Stream.fromSubscription(subscription).pipe(
            Stream.runForEach((change) =>
              Effect.sync(() => {
                if (change.threadId === threadId) Queue.offerUnsafe(mailbox, change.suggestion);
              }),
            ),
            Effect.forkScoped,
          );
        }),
      { bufferSize: 1, strategy: "sliding" },
    );

  const service: ThreadPromptSuggestionsShape = { set, clear, stream };
  return service;
});

export const layer = Layer.effect(ThreadPromptSuggestions, make);
