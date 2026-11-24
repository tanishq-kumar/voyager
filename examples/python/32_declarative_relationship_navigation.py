"""Voyager OGM: Declarative Relationship Navigation.

Teaches:
    - Declaring `RelationshipTo` / `RelationshipFrom` attributes on Node models
    - Class access returns the descriptor; instance access returns a manager
    - Navigating with `.all()` and filtering with `.filter(...)`
    - Staging `.connect(...)` / `.disconnect(...)` and writing on `session.flush()`
    - Discarding staged relationship operations on unit-of-work rollback
    - Identity-map hydration: the same node is the same Python object
    - String forward references, including circular model references

Prerequisites:
    - Engine: None (offline / in-memory using a recording MockBridge)
    - Extras: None

Run with:
    `uv run python examples/python/32_declarative_relationship_navigation.py`
"""

from __future__ import annotations

from typing import Any

from voyager_ogm import (
    Field,
    MockBridge,
    Node,
    Relationship,
    RelationshipFrom,
    RelationshipTo,
    Session,
    node,
    relationship,
)


@relationship(type_name="KNOWS")
class Knows(Relationship):
    since: int


@node(label="Person")
class Person(Node):
    id: str = Field(primary_key=True)
    name: str
    age: int = 0

    friends: RelationshipTo[Person] = RelationshipTo(target="Person", rel_type="KNOWS", model=Knows)
    followers: RelationshipFrom[Person] = RelationshipFrom(target="Person", rel_type="FOLLOWS")


@node(label="Team")
class Team(Node):
    id: str = Field(primary_key=True)
    name: str

    # Circular forward reference: Team mentions Person before Person is defined.
    members: RelationshipFrom[Person] = RelationshipFrom(target="Person", rel_type="MEMBER_OF")


class RecordingBridge(MockBridge):
    """Answers navigation RETURN queries with canned projected property rows."""

    def __init__(self) -> None:
        super().__init__()
        self.node_rows: list[dict[str, Any]] = []

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        super().execute(statement, parameters)
        marker = " RETURN "
        if marker not in statement or not self.node_rows:
            return []
        return list(self.node_rows)


def main() -> None:
    print("=== Voyager OGM: Declarative Relationship Navigation ===\n")

    bridge = RecordingBridge()
    session = Session(bridge=bridge, dialect="cypher", enable_identity_map=True)

    alice = Person(id="alice", name="Alice", age=30, session=session)
    bob = Person(id="bob", name="Bob", age=18, session=session)
    carol = Person(id="carol", name="Carol", age=16, session=session)

    print("--- [1] Descriptor vs manager (no I/O on attribute access) ---")
    print(f"Class access:    {Person.friends!r}")
    print(f"Instance access: {type(alice.friends).__name__}")
    print(f"Queries executed so far: {len(bridge.executed_queries)}\n")

    print("--- [2] Navigate with .all() ---")
    bridge.node_rows = [
        {"id": "bob", "name": "Bob", "age": 18},
        {"id": "carol", "name": "Carol", "age": 16},
    ]
    friends = alice.friends.all()
    friend_names = [person.get("name") for person in friends]
    print(f"friends: {friend_names}")
    print(f"Statement: {bridge.executed_queries[-1][0]}")
    print(f"Identity map reuse: bob is same object -> {friends[0] is bob}\n")

    print("--- [3] Filter with a fluent predicate ---")
    bridge.node_rows = [{"id": "bob", "name": "Bob", "age": 18}]
    adults = alice.friends.filter(Person.age >= 18)
    adult_names = [person.get("name") for person in adults]
    print(f"adults: {adult_names}")
    print(f"Statement: {bridge.executed_queries[-1][0]}\n")

    print("--- [4] Stage connect / disconnect, write on flush ---")
    alice.friends.connect(bob, properties={"since": 2026})
    alice.followers.connect(carol)
    print(f"Staged operations: {len(session._pending_relationships)}")
    session.flush()
    for statement, parameters in bridge.executed_queries[-2:]:
        print(f"Flush: {statement}  parameters={parameters}")

    alice.friends.disconnect(bob)
    session.flush()
    print(f"Disconnect: {bridge.executed_queries[-1][0]}\n")

    print("--- [5] Rollback discards staged operations ---")
    try:
        with session.unit_of_work():
            alice.friends.connect(carol)
            print(f"Inside unit of work: {len(session._pending_relationships)} staged")
            raise RuntimeError("simulated failure")
    except RuntimeError as exc:
        print(f"Caught expected error: {exc}")
    print(f"After rollback: {len(session._pending_relationships)} staged\n")

    print("--- [6] Circular forward references ---")
    print(f"Team.members resolves to: {Team.members.resolve().__name__}")
    print(f"Person.friends resolves to: {Person.friends.resolve().__name__}\n")

    print("[SUCCESS] Declarative relationship navigation demonstration completed.")


if __name__ == "__main__":
    main()
