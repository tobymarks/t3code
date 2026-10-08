import * as Schema from "effect/Schema";
import { ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";

/**
 * A provider's prediction of the user's next prompt, offered after a turn
 * settles. Memory only: the server holds the latest one per thread until the
 * next turn starts or the session ends, and never persists it.
 */
export const ThreadPromptSuggestion = Schema.Struct({
  threadId: ThreadId,
  text: TrimmedNonEmptyString,
});
export type ThreadPromptSuggestion = typeof ThreadPromptSuggestion.Type;

export const ThreadPromptSuggestionSubscribeInput = Schema.Struct({
  threadId: ThreadId,
});
export type ThreadPromptSuggestionSubscribeInput = typeof ThreadPromptSuggestionSubscribeInput.Type;

/** Null means no suggestion for that thread. Sent first, then after every change. */
export const ThreadPromptSuggestionStreamEvent = Schema.NullOr(ThreadPromptSuggestion);
export type ThreadPromptSuggestionStreamEvent = typeof ThreadPromptSuggestionStreamEvent.Type;
