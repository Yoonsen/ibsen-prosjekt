from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass
from typing import Iterable

WORD_RE = re.compile(r"[A-Za-zÆØÅæøå]+(?:[-'][A-Za-zÆØÅæøå]+)?")
SENTENCE_SPLIT_RE = re.compile(r"[.!?]+")

# Compact stopword set for first-pass filtering.
STOPWORDS_NB = {
    "a",
    "at",
    "av",
    "ble",
    "bli",
    "blir",
    "da",
    "de",
    "dem",
    "den",
    "der",
    "det",
    "du",
    "eg",
    "eller",
    "en",
    "er",
    "et",
    "for",
    "fra",
    "han",
    "har",
    "hun",
    "hva",
    "hvem",
    "hvor",
    "i",
    "ikke",
    "inn",
    "jeg",
    "kan",
    "kom",
    "man",
    "med",
    "meg",
    "men",
    "mot",
    "må",
    "nei",
    "nå",
    "når",
    "og",
    "om",
    "opp",
    "oss",
    "på",
    "seg",
    "sin",
    "som",
    "så",
    "til",
    "under",
    "ut",
    "var",
    "ved",
    "vi",
    "vil",
    "vår",
    "være",
    "å",
}


@dataclass(frozen=True)
class InfoDensityConfig:
    n_min: int = 2
    n_max: int = 6
    threshold: float = 14.0
    alpha: float = 0.1
    top_k: int = 100
    min_occurrences: int = 1


@dataclass(frozen=True)
class PhraseScore:
    phrase: str
    n: int
    info_score: float
    count_background: int
    probability: float
    occurrences_in_selection: int
    sample_sentence: str


class LocalNgramBackend:
    def __init__(self, texts: Iterable[str], n_max: int = 6) -> None:
        self.n_max = n_max
        self.ngram_counts = {n: Counter() for n in range(1, n_max + 1)}
        self.total_by_n = {n: 0 for n in range(1, n_max + 1)}
        self.vocab_by_n = {n: 0 for n in range(1, n_max + 1)}
        self._build(texts)

    def _build(self, texts: Iterable[str]) -> None:
        for text in texts:
            for sent in split_sentences(text):
                tokens = tokenize(sent)
                if not tokens:
                    continue
                token_count = len(tokens)
                for n in range(1, self.n_max + 1):
                    if token_count < n:
                        continue
                    self.total_by_n[n] += token_count - n + 1
                    for i in range(0, token_count - n + 1):
                        gram = tuple(tokens[i : i + n])
                        self.ngram_counts[n][gram] += 1
        for n in range(1, self.n_max + 1):
            self.vocab_by_n[n] = len(self.ngram_counts[n]) or 1

    def count(self, gram: tuple[str, ...]) -> int:
        n = len(gram)
        if n < 1 or n > self.n_max:
            return 0
        return self.ngram_counts[n].get(gram, 0)

    def probability(self, gram: tuple[str, ...], alpha: float) -> float:
        n = len(gram)
        c = self.count(gram)
        total = self.total_by_n[n]
        vocab = self.vocab_by_n[n]
        return (c + alpha) / (total + alpha * vocab)


def split_sentences(text: str) -> list[str]:
    chunks = SENTENCE_SPLIT_RE.split(text)
    return [chunk.strip() for chunk in chunks if chunk.strip()]


def tokenize(text: str) -> list[str]:
    return [m.group(0).lower() for m in WORD_RE.finditer(text)]


def should_skip_ngram(gram: tuple[str, ...], stopwords: set[str]) -> bool:
    return bool(gram) and gram[0] in stopwords and gram[-1] in stopwords


def analyze_text(
    text: str,
    backend: LocalNgramBackend,
    config: InfoDensityConfig,
    stopwords: set[str] | None = None,
) -> tuple[list[PhraseScore], dict[str, int]]:
    stopword_set = stopwords or STOPWORDS_NB
    phrase_state: dict[tuple[str, int], dict[str, object]] = {}

    for sentence in split_sentences(text):
        tokens = tokenize(sentence)
        token_count = len(tokens)
        if token_count < config.n_min:
            continue

        upper_n = min(config.n_max, token_count)
        for n in range(config.n_min, upper_n + 1):
            for i in range(0, token_count - n + 1):
                gram = tuple(tokens[i : i + n])
                if should_skip_ngram(gram, stopword_set):
                    continue

                p = backend.probability(gram, alpha=config.alpha)
                score = -math.log2(p)
                if score < config.threshold:
                    continue

                phrase = " ".join(gram)
                key = (phrase, n)
                if key not in phrase_state:
                    phrase_state[key] = {
                        "phrase": phrase,
                        "n": n,
                        "score": score,
                        "count_background": backend.count(gram),
                        "probability": p,
                        "occurrences": 0,
                        "sample_sentence": sentence,
                    }
                phrase_state[key]["occurrences"] = int(phrase_state[key]["occurrences"]) + 1

    scores: list[PhraseScore] = []
    histogram: Counter[int] = Counter()
    for info in phrase_state.values():
        occ = int(info["occurrences"])
        if occ < config.min_occurrences:
            continue
        score = float(info["score"])
        histogram[int(math.floor(score))] += 1
        scores.append(
            PhraseScore(
                phrase=str(info["phrase"]),
                n=int(info["n"]),
                info_score=score,
                count_background=int(info["count_background"]),
                probability=float(info["probability"]),
                occurrences_in_selection=occ,
                sample_sentence=str(info["sample_sentence"]),
            )
        )

    scores.sort(
        key=lambda x: (x.info_score, x.occurrences_in_selection, x.n, x.phrase),
        reverse=True,
    )
    return scores[: config.top_k], dict(sorted(histogram.items()))
