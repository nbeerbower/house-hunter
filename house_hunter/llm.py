import json
import os
import re
import sys
from typing import Callable, Optional

import litellm

from house_hunter.config import LLMConfig
from house_hunter.prompts import (
    build_comparison_prompt,
    build_scoring_system_prompt,
    build_scoring_user_prompt,
    format_listing_batch,
)

# Suppress litellm's verbose logging
litellm.suppress_debug_info = True


class LLM:
    def __init__(self, config: LLMConfig):
        self.config = config
        self.debug = os.environ.get("HOUSE_HUNTER_DEBUG", "").lower() in ("1", "true", "yes")
        # Force-set OPENAI_API_KEY for local servers. litellm passes api_key
        # in kwargs but the OpenAI SDK client constructor also independently
        # checks the env var and blows up if it's missing.
        if config.is_local and not os.environ.get("OPENAI_API_KEY"):
            os.environ["OPENAI_API_KEY"] = "not-needed"

    def _debug_log(self, label: str, text: str):
        if not self.debug:
            return
        separator = "─" * 60
        print(f"\n{separator}", file=sys.stderr)
        print(f"[DEBUG LLM] {label}", file=sys.stderr)
        print(separator, file=sys.stderr)
        print(text, file=sys.stderr)
        print(separator, file=sys.stderr)

    def complete(self, system: str, user: str) -> str:
        self._debug_log("SYSTEM PROMPT", system)
        self._debug_log("USER PROMPT", user)
        kwargs = dict(
            model=self.config.litellm_model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=self.config.temperature,
            max_tokens=self.config.max_tokens,
        )
        if self.config.api_base:
            kwargs["api_base"] = self.config.api_base
        # For local servers, always pass a dummy key so litellm/openai SDK
        # doesn't blow up. The actual api_key config takes priority if set.
        if self.config.api_key:
            kwargs["api_key"] = self.config.api_key
        elif self.config.is_local:
            kwargs["api_key"] = "not-needed"
        response = litellm.completion(**kwargs)
        content = response.choices[0].message.content
        self._debug_log("LLM RESPONSE", content)
        usage = getattr(response, "usage", None)
        if usage and self.debug:
            self._debug_log("USAGE", f"prompt_tokens={usage.prompt_tokens}  completion_tokens={usage.completion_tokens}  total={usage.total_tokens}")
        return content

    def chat(self, system: str, user_message: str) -> str:
        return self.complete(system, user_message)

    def score_listings(
        self,
        listings: list[dict],
        preferences: list[str],
        favorites: list[dict],
        rejections_summary: str,
        start_index: int = 1,
        distances_map: dict[str, list[dict]] | None = None,
        districts_map: dict[str, dict] | None = None,
        distance_locations: list[dict] | None = None,
    ) -> list[dict]:
        """Score a single batch of listings. Returns list of {property_id, score, reasoning}."""
        listings_text = format_listing_batch(listings, start_index, distances_map, districts_map)
        system_prompt = build_scoring_system_prompt(preferences, favorites, rejections_summary, distance_locations)
        user_prompt = build_scoring_user_prompt(listings_text)

        response_text = self.complete(system_prompt, user_prompt)
        parsed = self._parse_scores_json(response_text, listings, start_index)
        return parsed

    def score_all_listings(
        self,
        listings: list[dict],
        preferences: list[str],
        favorites: list[dict],
        rejections_summary: str,
        cached_scores: dict[str, dict],
        on_progress: Optional[Callable[[int, int], None]] = None,
        distances_map: dict[str, list[dict]] | None = None,
        districts_map: dict[str, dict] | None = None,
        distance_locations: list[dict] | None = None,
    ) -> list[dict]:
        """Score all listings, using cache where available. Returns sorted by score desc."""
        # Split into cached and uncached
        uncached = []
        results = []

        for l in listings:
            pid = l["property_id"]
            if pid in cached_scores:
                results.append({
                    "property_id": pid,
                    "score": cached_scores[pid]["score"],
                    "reasoning": cached_scores[pid].get("reasoning", ""),
                })
            else:
                uncached.append(l)

        if not uncached:
            results.sort(key=lambda x: x["score"], reverse=True)
            return results

        # Batch and score uncached listings
        batch_size = self.config.batch_size
        total_batches = (len(uncached) + batch_size - 1) // batch_size

        for batch_num in range(total_batches):
            start = batch_num * batch_size
            end = start + batch_size
            batch = uncached[start:end]

            if on_progress:
                on_progress(batch_num + 1, total_batches)

            try:
                batch_scores = self.score_listings(
                    batch, preferences, favorites, rejections_summary,
                    start_index=start + 1,
                    distances_map=distances_map,
                    districts_map=districts_map,
                    distance_locations=distance_locations,
                )
                results.extend(batch_scores)
            except Exception as e:
                # On failure, assign neutral scores
                print(f"  Warning: Scoring batch {batch_num + 1} failed: {e}")
                for l in batch:
                    results.append({
                        "property_id": l["property_id"],
                        "score": 5.0,
                        "reasoning": f"Scoring failed: {e}",
                    })

        results.sort(key=lambda x: x["score"], reverse=True)
        return results

    def compare(self, listings: list[dict]) -> str:
        """LLM-powered comparison of specific listings."""
        prompt = build_comparison_prompt(listings)
        return self.complete(
            "You are a real estate analyst. Be concise and practical.",
            prompt,
        )

    def classify_intent(self, user_input: str) -> dict:
        """Classify natural language input into an intent."""
        response = self.complete(
            """Classify the user's intent. Respond with ONLY a JSON object:
- {"intent": "preference", "text": "<the preference>"} — if they're stating what they want in a home
- {"intent": "question", "text": "<the question>"} — if they're asking about properties or the market
- {"intent": "search", "location": "<location>"} — if they want to search a new area
- {"intent": "unknown", "text": "<original text>"} — if unclear""",
            user_input,
        )
        try:
            return json.loads(self._strip_markdown_fences(response))
        except (json.JSONDecodeError, TypeError):
            return {"intent": "unknown", "text": user_input}

    def _parse_scores_json(
        self, response_text: str, listings: list[dict], start_index: int
    ) -> list[dict]:
        """Robustly parse scoring JSON from LLM response."""
        text = self._strip_markdown_fences(response_text)

        # Try direct parse
        parsed = self._try_parse_json(text)
        if parsed is None:
            # Try to extract JSON array with regex
            match = re.search(r'\[.*\]', text, re.DOTALL)
            if match:
                parsed = self._try_parse_json(match.group())

        if parsed is None:
            # Last resort: assign default scores
            return [
                {"property_id": l["property_id"], "score": 5.0, "reasoning": "Failed to parse LLM response"}
                for l in listings
            ]

        # Map parsed scores back to property IDs
        index_to_pid = {start_index + i: l["property_id"] for i, l in enumerate(listings)}
        results = []
        matched_pids = set()

        if isinstance(parsed, list):
            for item in parsed:
                if not isinstance(item, dict):
                    continue
                idx = item.get("index")
                pid = index_to_pid.get(idx)
                if pid and pid not in matched_pids:
                    pros = item.get("pros", [])
                    cons = item.get("cons", [])
                    summary = item.get("summary", "")
                    reasoning_parts = []
                    if summary:
                        reasoning_parts.append(summary)
                    if pros:
                        reasoning_parts.append("Pros: " + ", ".join(pros))
                    if cons:
                        reasoning_parts.append("Cons: " + ", ".join(cons))

                    results.append({
                        "property_id": pid,
                        "score": min(10, max(0, float(item.get("score", 5.0)))),
                        "reasoning": " | ".join(reasoning_parts),
                    })
                    matched_pids.add(pid)

        # Fill in any missing listings with default score
        for l in listings:
            if l["property_id"] not in matched_pids:
                results.append({
                    "property_id": l["property_id"],
                    "score": 5.0,
                    "reasoning": "Not scored by LLM",
                })

        return results

    @staticmethod
    def _strip_markdown_fences(text: str) -> str:
        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            # Remove first line (```json or ```) and last line (```)
            lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            text = "\n".join(lines)
        return text.strip()

    @staticmethod
    def _try_parse_json(text: str):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # Try fixing trailing commas
            fixed = re.sub(r',\s*([}\]])', r'\1', text)
            try:
                return json.loads(fixed)
            except json.JSONDecodeError:
                return None
