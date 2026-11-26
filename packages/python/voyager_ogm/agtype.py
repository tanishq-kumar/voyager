"""Decode Apache AGE ``agtype`` text into Python values.

AGE returns every projected value as a PostgreSQL ``agtype`` text column. A
vertex is a JSON object followed by ``::vertex``, an edge by ``::edge`` and a
path by ``::path``; scalars, lists and maps are JSON with an optional
``::numeric`` annotation. This module mirrors the Rust parser in
``voyager-drivers`` so the psycopg bridge returns the same Python shapes the
native backend produces.

Entities are recognised only by their annotation, so a plain map with ``id``,
``label`` and ``properties`` keys stays a map and an unannotated list such as
``[a, r, b]`` stays a list.
"""

from __future__ import annotations

import json
from typing import Any

from voyager_ogm.entities import GraphNode, GraphPath, GraphRelationship


class _AgtypeParser:
    """Quote-aware recursive-descent parser for one AGE ``agtype`` value."""

    def __init__(self, text: str) -> None:
        self.text = text
        self.pos = 0
        self.length = len(text)

    def skip_whitespace(self) -> None:
        """Skips JSON whitespace between tokens."""
        while self.pos < self.length and self.text[self.pos] in " \t\r\n":
            self.pos += 1

    def parse_value(self) -> Any:
        """Parses one value at the current position."""
        self.skip_whitespace()
        if self.pos >= self.length:
            return None
        char = self.text[self.pos]
        if char == '"':
            return self.parse_string()
        if char == "{":
            return self.parse_object()
        if char == "[":
            return self.parse_array()
        if char == "t" and self._consume_keyword("true"):
            return True
        if char == "f" and self._consume_keyword("false"):
            return False
        if char == "n" and self._consume_keyword("null"):
            return None
        if char == "N" and self._consume_keyword("NaN"):
            return float("nan")
        if char == "I" and self._consume_keyword("Infinity"):
            return float("inf")
        if char in "-+0123456789":
            return self.parse_number()
        remainder = self.text[self.pos :]
        self.pos = self.length
        return remainder

    def parse_string(self) -> str:
        """Parses one JSON string starting at the opening quote."""
        start = self.pos
        self.pos += 1
        escaped = False
        while self.pos < self.length:
            char = self.text[self.pos]
            self.pos += 1
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                break
        return json.loads(self.text[start : self.pos])

    def _consume_keyword(self, keyword: str) -> bool:
        """Consumes a whole keyword, rejecting a longer identifier prefix."""
        if not self.text.startswith(keyword, self.pos):
            return False
        next_pos = self.pos + len(keyword)
        if next_pos < self.length and (self.text[next_pos].isalnum() or self.text[next_pos] == "_"):
            return False
        self.pos = next_pos
        return True

    def parse_number(self) -> Any:
        """Parses an integer, float (including Inf/-Inf), or ``::numeric`` scalar."""
        if self.text.startswith("-Infinity", self.pos) and self._consume_keyword("-Infinity"):
            return float("-inf")
        if self.text.startswith("+Infinity", self.pos) and self._consume_keyword("+Infinity"):
            return float("inf")
        start = self.pos
        while self.pos < self.length and self.text[self.pos] in "-+0123456789.eE":
            self.pos += 1
        number_text = self.text[start : self.pos]
        self.skip_whitespace()
        if self.text.startswith("::numeric", self.pos):
            self.pos += len("::numeric")
        try:
            if "." in number_text or "e" in number_text.lower():
                return float(number_text)
            return int(number_text)
        except ValueError:
            return number_text

    def parse_object(self) -> Any:
        """Parses an object, turning it into a node or relationship only when annotated."""
        self.pos += 1
        self.skip_whitespace()
        result: dict[str, Any] = {}
        if self.pos < self.length and self.text[self.pos] == "}":
            self.pos += 1
        else:
            while self.pos < self.length:
                self.skip_whitespace()
                if self.pos >= self.length or self.text[self.pos] != '"':
                    break
                key = self.parse_string()
                self.skip_whitespace()
                if self.pos < self.length and self.text[self.pos] == ":":
                    self.pos += 1
                result[key] = self.parse_value()
                self.skip_whitespace()
                if self.pos < self.length and self.text[self.pos] == ",":
                    self.pos += 1
                    continue
                if self.pos < self.length and self.text[self.pos] == "}":
                    self.pos += 1
                    break

        self.skip_whitespace()
        if self.text.startswith("::vertex", self.pos):
            self.pos += len("::vertex")
            label = result.get("label")
            labels = frozenset({str(label)}) if label else frozenset()
            props = dict(result.get("properties") or {})
            return GraphNode._from_trusted(
                node_id=result.get("id"),
                element_id=None,
                labels=labels,
                properties=props,
            )
        if self.text.startswith("::edge", self.pos):
            self.pos += len("::edge")
            props = dict(result.get("properties") or {})
            return GraphRelationship._from_trusted(
                rel_id=result.get("id"),
                element_id=None,
                rel_type=str(result.get("label", "")),
                start_id=result.get("start_id"),
                end_id=result.get("end_id"),
                properties=props,
            )
        return result

    def parse_array(self) -> Any:
        """Parses an array, turning it into a path only when annotated."""
        self.pos += 1
        self.skip_whitespace()
        items: list[Any] = []
        if self.pos < self.length and self.text[self.pos] == "]":
            self.pos += 1
        else:
            while self.pos < self.length:
                items.append(self.parse_value())
                self.skip_whitespace()
                if self.pos < self.length and self.text[self.pos] == ",":
                    self.pos += 1
                    continue
                if self.pos < self.length and self.text[self.pos] == "]":
                    self.pos += 1
                    break

        self.skip_whitespace()
        if self.text.startswith("::path", self.pos):
            self.pos += len("::path")
            nodes: list[GraphNode] = []
            relationships: list[GraphRelationship] = []
            for index, item in enumerate(items):
                if index % 2 == 0:
                    if not isinstance(item, GraphNode):
                        raise ValueError(f"Expected a vertex at path index {index}")
                    nodes.append(item)
                else:
                    if not isinstance(item, GraphRelationship):
                        raise ValueError(f"Expected an edge at path index {index}")
                    relationships.append(item)
            return GraphPath._from_trusted(tuple(nodes), tuple(relationships))
        return items


def decode_agtype(text: str) -> Any:
    """Decodes one Apache AGE ``agtype`` text value.

    Args:
        text: Raw value as returned by PostgreSQL for an ``agtype`` column.

    Returns:
        The decoded Python value. Vertices, edges, and paths become
        :class:`~voyager_ogm.entities.GraphNode`,
        :class:`~voyager_ogm.entities.GraphRelationship`, and
        :class:`~voyager_ogm.entities.GraphPath`. Lists and maps recurse, and
        scalars keep their Python type.

    Raises:
        ValueError: If the text is not a well-formed ``agtype`` value.
    """
    if text is None:
        return None
    parser = _AgtypeParser(text.strip())
    value = parser.parse_value()
    parser.skip_whitespace()
    if parser.pos < parser.length:
        raise ValueError(f"Unexpected trailing agtype text at position {parser.pos}")
    return value


__all__ = ["decode_agtype"]
