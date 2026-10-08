import { describe, expect, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";

import * as ThreadPromptSuggestions from "./ThreadPromptSuggestions.ts";

const threadId = ThreadId.make("thread-1");
const otherThreadId = ThreadId.make("thread-2");

describe("ThreadPromptSuggestions", () => {
  it.effect("streams the current suggestion first, then changes for that thread only", () =>
    Effect.gen(function* () {
      const store = yield* ThreadPromptSuggestions.make;
      yield* store.set(threadId, "  Run the tests  ");

      // Change the store only once the subscriber holds its first value.
      const subscribed = yield* Deferred.make<void>();
      const received = yield* store.stream(threadId).pipe(
        Stream.tap(() => Deferred.succeed(subscribed, undefined)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* Deferred.await(subscribed);
      yield* store.set(otherThreadId, "Not for this thread");
      yield* store.clear(threadId);

      expect(yield* Fiber.join(received)).toEqual([{ threadId, text: "Run the tests" }, null]);
      expect(yield* store.stream(otherThreadId).pipe(Stream.take(1), Stream.runCollect)).toEqual([
        { threadId: otherThreadId, text: "Not for this thread" },
      ]);
    }),
  );

  it.effect("treats a blank suggestion as none", () =>
    Effect.gen(function* () {
      const store = yield* ThreadPromptSuggestions.make;
      yield* store.set(threadId, "Run the tests");
      yield* store.set(threadId, "   ");

      expect(yield* store.stream(threadId).pipe(Stream.take(1), Stream.runCollect)).toEqual([null]);
    }),
  );
});
