"""Unit and integration tests for Field value constraints (#282).

Covers:
- Value constraints at boundary: pattern, ge, gt, le, lt, min_length, max_length, choices.
- bool handling for numeric bounds (bool must not pass as int).
- None handling: None is accepted on nullable=False and nullable=True fields, with and without constraints.
- Attribute assignment validation after instance creation.
- Save paths: Node.save, Node.async_save, Session.register, Session.flush using MockBridge.
- Declaration-time errors when class is defined.
- Error attributes (model, field, constraint, value) and message structure.
- Relationship entity constraints.
- JSON Schema keyword mapping (#125).
"""

from __future__ import annotations

import re

import pytest
from voyager_ogm import (
    AsyncSession,
    Field,
    FieldConstraintError,
    Node,
    Relationship,
    SchemaValidationError,
    Session,
    node,
)
from voyager_ogm.bridge import AsyncMockBridge, MockBridge


class TestFieldConstraintErrorType:
    """Verifies FieldConstraintError structure and backward compatibility."""

    def test_inheritance_and_error_type(self) -> None:
        import voyager_ogm
        import voyager_ogm.models
        import voyager_ogm.query
        from voyager_ogm.query import SchemaValidationError as QuerySchemaValidationError

        assert voyager_ogm.models.SchemaValidationError is voyager_ogm.SchemaValidationError
        assert QuerySchemaValidationError is voyager_ogm.SchemaValidationError

        err = FieldConstraintError(
            "Person", "age", "ge", -1, "Person.age: value -1 violates ge >= 0"
        )
        assert isinstance(err, SchemaValidationError)
        assert isinstance(err, ValueError)
        assert isinstance(err, FieldConstraintError)

    def test_error_attributes_and_message(self) -> None:
        class Person(Node):
            age: int = Field(ge=0)

        with pytest.raises(FieldConstraintError) as exc_info:
            Person(age=-5)

        err = exc_info.value
        assert err.model is Person
        assert err.model_name == "Person"
        assert err.field == "age"
        assert err.constraint == "ge"
        assert err.value == -5
        assert "Person.age" in str(err)
        assert "ge" in str(err)


class TestPatternConstraint:
    """Tests regex pattern constraint on string fields."""

    def test_pattern_boundary_accept_and_reject(self) -> None:
        class User(Node):
            email: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

        # Accept exact fullmatch
        u = User(email="ada@example.com")
        assert u.get("email") == "ada@example.com"

        # Reject invalid format
        with pytest.raises(FieldConstraintError) as exc:
            User(email="invalid-email")
        assert exc.value.constraint == "pattern"
        assert exc.value.value == "invalid-email"

        # Reject partial match (pattern must fully match)
        with pytest.raises(FieldConstraintError) as exc:
            User(email="prefix ada@example.com")
        assert exc.value.constraint == "pattern"

        with pytest.raises(FieldConstraintError) as exc:
            User(email="ada@example.com postfix")
        assert exc.value.constraint == "pattern"

    def test_pattern_compiled_regex_instance(self) -> None:
        regex = re.compile(r"^[A-Z]{3}-\d{3}$")

        class Code(Node):
            code: str = Field(pattern=regex)

        c = Code(code="ABC-123")
        assert c.get("code") == "ABC-123"

        with pytest.raises(FieldConstraintError):
            Code(code="abc-123")


class TestNumericBoundsConstraints:
    """Tests ge, gt, le, lt bounds and strict bool exclusion."""

    def test_ge_boundary(self) -> None:
        class Account(Node):
            balance: int = Field(ge=10)

        # Accept at boundary
        a10 = Account(balance=10)
        assert a10.get("balance") == 10

        # Accept above boundary
        a11 = Account(balance=11)
        assert a11.get("balance") == 11

        # Reject below boundary
        with pytest.raises(FieldConstraintError) as exc:
            Account(balance=9)
        assert exc.value.constraint == "ge"
        assert exc.value.value == 9

    def test_gt_boundary(self) -> None:
        class Product(Node):
            price: float = Field(gt=10.0)

        # Reject at boundary
        with pytest.raises(FieldConstraintError) as exc:
            Product(price=10.0)
        assert exc.value.constraint == "gt"

        # Accept above boundary
        p = Product(price=10.001)
        assert p.get("price") == 10.001

        # Reject below boundary
        with pytest.raises(FieldConstraintError) as exc:
            Product(price=9.999)
        assert exc.value.constraint == "gt"

    def test_le_boundary(self) -> None:
        class Rating(Node):
            score: int = Field(le=5)

        # Accept at boundary
        r5 = Rating(score=5)
        assert r5.get("score") == 5

        # Accept below boundary
        r4 = Rating(score=4)
        assert r4.get("score") == 4

        # Reject above boundary
        with pytest.raises(FieldConstraintError) as exc:
            Rating(score=6)
        assert exc.value.constraint == "le"
        assert exc.value.value == 6

    def test_lt_boundary(self) -> None:
        class Discount(Node):
            pct: float = Field(lt=1.0)

        # Reject at boundary
        with pytest.raises(FieldConstraintError) as exc:
            Discount(pct=1.0)
        assert exc.value.constraint == "lt"

        # Accept below boundary
        d = Discount(pct=0.999)
        assert d.get("pct") == 0.999

        # Reject above boundary
        with pytest.raises(FieldConstraintError) as exc:
            Discount(pct=1.001)
        assert exc.value.constraint == "lt"

    def test_bool_cannot_pass_as_int_for_numeric_bounds(self) -> None:
        """Requirement 2: bool must not pass as an int for ge/gt/le/lt."""

        class Counter(Node):
            count: int = Field(ge=0, le=5)

        # Passing True or False directly must be rejected, not treated as 1 or 0
        with pytest.raises(FieldConstraintError) as exc_true:
            Counter(count=True)
        assert exc_true.value.constraint == "ge"
        assert exc_true.value.value is True

        with pytest.raises(FieldConstraintError) as exc_false:
            Counter(count=False)
        assert exc_false.value.constraint == "ge"
        assert exc_false.value.value is False

        # Assignment of bool must also be rejected
        c = Counter(count=2)
        with pytest.raises(FieldConstraintError):
            c.count = True


class TestLengthConstraints:
    """Tests min_length and max_length on str and list fields."""

    def test_string_length_boundaries(self) -> None:
        class Article(Node):
            title: str = Field(min_length=3, max_length=5)

        # Accept at lower boundary
        a3 = Article(title="abc")
        assert a3.get("title") == "abc"

        # Accept between boundaries
        a4 = Article(title="abcd")
        assert a4.get("title") == "abcd"

        # Accept at upper boundary
        a5 = Article(title="abcde")
        assert a5.get("title") == "abcde"

        # Reject below lower boundary
        with pytest.raises(FieldConstraintError) as exc:
            Article(title="ab")
        assert exc.value.constraint == "min_length"
        assert exc.value.value == "ab"

        # Reject above upper boundary
        with pytest.raises(FieldConstraintError) as exc:
            Article(title="abcdef")
        assert exc.value.constraint == "max_length"
        assert exc.value.value == "abcdef"

    def test_list_length_boundaries(self) -> None:
        class Team(Node):
            members: list[str] = Field(min_length=1, max_length=3)

        # Accept at lower boundary
        t1 = Team(members=["Alice"])
        assert t1.get("members") == ["Alice"]

        # Accept at upper boundary
        t3 = Team(members=["A", "B", "C"])
        assert t3.get("members") == ["A", "B", "C"]

        # Reject below lower boundary (empty list)
        with pytest.raises(FieldConstraintError) as exc:
            Team(members=[])
        assert exc.value.constraint == "min_length"

        # Reject above upper boundary
        with pytest.raises(FieldConstraintError) as exc:
            Team(members=["A", "B", "C", "D"])
        assert exc.value.constraint == "max_length"


class TestChoicesConstraint:
    """Tests choices constraint and equality comparison."""

    def test_choices_strings(self) -> None:
        class Ticket(Node):
            status: str = Field(choices=["open", "in_progress", "closed"])

        t = Ticket(status="open")
        assert t.get("status") == "open"

        with pytest.raises(FieldConstraintError) as exc:
            Ticket(status="cancelled")
        assert exc.value.constraint == "choices"
        assert exc.value.value == "cancelled"

    def test_choices_numbers(self) -> None:
        class Level(Node):
            priority: int = Field(choices=[1, 2, 3])

        lvl = Level(priority=2)
        assert lvl.get("priority") == 2

        with pytest.raises(FieldConstraintError) as exc:
            Level(priority=4)
        assert exc.value.constraint == "choices"


class TestNullableAndNoneHandling:
    """Tests None handling: None skips value constraints regardless of nullable."""

    def test_none_accepted_on_creation(self) -> None:
        class Item(Node):
            # nullable=False, with and without constraints
            req_plain: str = Field(nullable=False)
            req_constrained: int = Field(nullable=False, ge=10)
            # nullable=True, with and without constraints
            opt_plain: str = Field(nullable=True)
            opt_constrained: str = Field(nullable=True, pattern=r"^[A-Z]+$")

        # None is accepted on creation for all fields
        item = Item(
            req_plain=None,
            req_constrained=None,
            opt_plain=None,
            opt_constrained=None,
        )
        assert item.get("req_plain") is None
        assert item.get("req_constrained") is None
        assert item.get("opt_plain") is None
        assert item.get("opt_constrained") is None

    def test_none_accepted_on_assignment(self) -> None:
        class Item(Node):
            req_plain: str = Field(nullable=False)
            req_constrained: int = Field(nullable=False, ge=10)
            opt_plain: str = Field(nullable=True)
            opt_constrained: str = Field(nullable=True, pattern=r"^[A-Z]+$")

        item = Item(
            req_plain="hello",
            req_constrained=20,
            opt_plain="world",
            opt_constrained="VALID",
        )
        # Reassigning None is accepted for all fields
        item.req_plain = None
        item.req_constrained = None
        item.opt_plain = None
        item.opt_constrained = None

        assert item.get("req_plain") is None
        assert item.get("req_constrained") is None
        assert item.get("opt_plain") is None
        assert item.get("opt_constrained") is None

    def test_none_accepted_on_save_mock_bridge(self) -> None:
        class Item(Node):
            item_id: str = Field(primary_key=True)
            req_plain: str = Field(nullable=False)
            req_constrained: int = Field(nullable=False, ge=10)
            opt_plain: str = Field(nullable=True)
            opt_constrained: str = Field(nullable=True, pattern=r"^[A-Z]+$")

        mock = MockBridge()
        session = Session(bridge=mock, enable_identity_map=True)

        item = Item(
            item_id="i1",
            req_plain=None,
            req_constrained=None,
            opt_plain=None,
            opt_constrained=None,
        )
        session.register(item)
        res = item.save()
        assert res is not None
        assert len(mock.executed_queries) == 1

        # Now mutate to valid then back to None and save
        item.req_constrained = 50
        item.save()
        assert len(mock.executed_queries) == 2

        item.req_constrained = None
        item.save()
        assert len(mock.executed_queries) == 3

    @pytest.mark.asyncio
    async def test_none_accepted_on_async_save_mock_bridge(self) -> None:
        class Item(Node):
            item_id: str = Field(primary_key=True)
            req_plain: str = Field(nullable=False)
            req_constrained: int = Field(nullable=False, ge=10)
            opt_plain: str = Field(nullable=True)
            opt_constrained: str = Field(nullable=True, pattern=r"^[A-Z]+$")

        async_mock = AsyncMockBridge()
        session = AsyncSession(bridge=async_mock, enable_identity_map=True)

        item = Item(
            item_id="i_async",
            req_plain=None,
            req_constrained=None,
            opt_plain=None,
            opt_constrained=None,
        )
        session.register(item)
        res = await item.async_save()
        assert res is not None
        assert len(async_mock.executed_queries) == 1

    def test_none_accepted_on_session_flush(self) -> None:
        class Item(Node):
            item_id: str = Field(primary_key=True)
            req_plain: str = Field(nullable=False)
            req_constrained: int = Field(nullable=False, ge=10)
            opt_plain: str = Field(nullable=True)
            opt_constrained: str = Field(nullable=True, pattern=r"^[A-Z]+$")

        mock = MockBridge()
        session = Session(bridge=mock, enable_identity_map=True)

        item = Item(
            item_id="i2",
            req_plain="init",
            req_constrained=15,
            opt_plain="init",
            opt_constrained="ABC",
        )
        session.register(item)
        # Change values to None
        item.req_plain = None
        item.req_constrained = None
        item.opt_plain = None
        item.opt_constrained = None

        res = session.flush()
        assert res is not None
        assert len(mock.executed_queries) == 1


class TestAttributeAssignmentValidation:
    """Tests attribute assignment checks and dirty state protection."""

    def test_assignment_rejection_leaves_state_uncorrupted(self) -> None:
        class Person(Node):
            age: int = Field(ge=0, le=120)

        p = Person(age=30)
        p.clear_dirty()

        # Reject invalid assignment
        with pytest.raises(FieldConstraintError):
            p.age = -1

        # Value remains untouched, dirty fields unchanged
        assert p.get("age") == 30
        assert p.dirty_fields == {}

        # Valid assignment updates values and dirty tracking
        p.age = 35
        assert p.get("age") == 35
        assert p.dirty_fields == {"age": 35}


class TestSaveAndFlushPaths:
    """Tests Node.save, Node.async_save, Session.register, Session.flush with MockBridge."""

    def test_node_save_validates_and_persists(self) -> None:
        class User(Node):
            user_id: str = Field(primary_key=True)
            age: int = Field(ge=0)

        mock = MockBridge()
        session = Session(bridge=mock, enable_identity_map=True)

        user = User(user_id="u1", age=20)
        session.register(user)

        user.age = 25
        result = user.save()
        assert result is not None
        assert len(mock.executed_queries) == 1

    def test_node_save_rejects_invalid_dirty_fields(self) -> None:
        class User(Node):
            user_id: str = Field(primary_key=True)
            age: int = Field(ge=0)

        mock = MockBridge()
        session = Session(bridge=mock, enable_identity_map=True)
        user = User(user_id="u1", age=20)
        session.register(user)

        # Bypass __setattr__ directly to simulate invalid state
        user._values["age"] = -5
        user._dirty_fields["age"] = -5

        with pytest.raises(FieldConstraintError) as exc:
            user.save()
        assert exc.value.constraint == "ge"
        assert len(mock.executed_queries) == 0

    @pytest.mark.asyncio
    async def test_node_async_save_validates(self) -> None:
        class User(Node):
            user_id: str = Field(primary_key=True)
            age: int = Field(ge=0)

        async_mock = AsyncMockBridge()
        session = AsyncSession(bridge=async_mock, enable_identity_map=True)
        user = User(user_id="u_async", age=22)
        session.register(user)

        user._values["age"] = -1
        user._dirty_fields["age"] = -1

        with pytest.raises(FieldConstraintError):
            await user.async_save()
        assert len(async_mock.executed_queries) == 0

    def test_session_register_validates_node(self) -> None:
        class User(Node):
            user_id: str = Field(primary_key=True)
            age: int = Field(ge=0)

        session = Session(enable_identity_map=True)
        user = User(user_id="u2", age=10)

        # Tamper with internal dict
        user._values["age"] = -99

        with pytest.raises(FieldConstraintError) as exc:
            session.register(user)
        assert exc.value.constraint == "ge"
        assert exc.value.value == -99

    def test_session_flush_validates_all_dirty_nodes(self) -> None:
        class User(Node):
            user_id: str = Field(primary_key=True)
            age: int = Field(ge=0)

        mock = MockBridge()
        session = Session(bridge=mock, enable_identity_map=True)

        user = User(user_id="u3", age=18)
        session.register(user)

        # Tamper
        user._values["age"] = -10
        user._dirty_fields["age"] = -10

        with pytest.raises(FieldConstraintError) as exc:
            session.flush()
        assert exc.value.constraint == "ge"
        assert len(mock.executed_queries) == 0


class TestDeclarationTimeErrors:
    """Tests invalid declarations raised when class is defined (Requirement 4)."""

    def test_uncompilable_pattern_raises(self) -> None:
        with pytest.raises(FieldConstraintError) as exc:

            class BadPattern(Node):
                code: str = Field(pattern=r"[unclosed-bracket")

        assert exc.value.constraint == "pattern"

    def test_ge_greater_than_le_raises(self) -> None:
        with pytest.raises(FieldConstraintError) as exc:

            class EmptyRange(Node):
                val: int = Field(ge=10, le=5)

        assert exc.value.constraint == "ge"

    def test_ge_greater_equal_lt_raises(self) -> None:
        with pytest.raises(FieldConstraintError) as exc:

            class EmptyRange(Node):
                val: int = Field(ge=10, lt=10)

        assert exc.value.constraint == "ge"

    def test_gt_greater_equal_le_raises(self) -> None:
        with pytest.raises(FieldConstraintError) as exc:

            class EmptyRange(Node):
                val: int = Field(gt=10, le=10)

        assert exc.value.constraint == "gt"

    def test_gt_greater_equal_lt_raises(self) -> None:
        with pytest.raises(FieldConstraintError) as exc:

            class EmptyRange(Node):
                val: int = Field(gt=10, lt=10)

        assert exc.value.constraint == "gt"

    def test_min_length_greater_than_max_length_raises(self) -> None:
        with pytest.raises(FieldConstraintError) as exc:

            class BadLength(Node):
                name: str = Field(min_length=10, max_length=5)

        assert exc.value.constraint == "min_length"

    def test_negative_lengths_raise(self) -> None:
        with pytest.raises(FieldConstraintError) as exc:

            class NegMin(Node):
                name: str = Field(min_length=-1)

        assert exc.value.constraint == "min_length"

        with pytest.raises(FieldConstraintError) as exc2:

            class NegMax(Node):
                name: str = Field(max_length=-5)

        assert exc2.value.constraint == "max_length"

    def test_empty_choices_raises(self) -> None:
        with pytest.raises(FieldConstraintError) as exc:

            class NoChoice(Node):
                status: str = Field(choices=[])

        assert exc.value.constraint == "choices"

    def test_pattern_on_non_string_type_raises(self) -> None:
        with pytest.raises(FieldConstraintError) as exc:

            class BadType(Node):
                age: int = Field(pattern=r"^\d+$")

        assert exc.value.constraint == "pattern"

    def test_ge_on_non_numeric_type_raises(self) -> None:
        with pytest.raises(FieldConstraintError) as exc:

            class BadType(Node):
                name: str = Field(ge=0)

        assert exc.value.constraint == "ge"

    def test_numeric_bounds_on_bool_type_raises(self) -> None:
        with pytest.raises(FieldConstraintError) as exc:

            class BadType(Node):
                is_active: bool = Field(ge=0)

        assert exc.value.constraint == "ge"
        assert "bool" in str(exc.value)

    def test_length_on_int_type_raises(self) -> None:
        with pytest.raises(FieldConstraintError) as exc:

            class BadType(Node):
                age: int = Field(min_length=1)

        assert exc.value.constraint == "min_length"

    def test_node_decorator_declaration_errors(self) -> None:
        with pytest.raises(FieldConstraintError):

            @node
            class BadDecorated:
                age: int = Field(ge=10, le=2)


class TestRelationshipConstraints:
    """Verifies that Relationship entities enforce field constraints identically."""

    def test_relationship_init_and_assignment_validation(self) -> None:
        class Authored(Relationship, type_name="AUTHORED"):
            weight: float = Field(ge=0.0, le=1.0)
            role: str = Field(choices=["lead", "contributor"])

        # Accept valid
        rel = Authored(weight=0.5, role="lead")
        assert rel.get("weight") == 0.5
        assert rel.get("role") == "lead"

        # Reject invalid init
        with pytest.raises(FieldConstraintError) as exc:
            Authored(weight=1.5, role="lead")
        assert exc.value.constraint == "le"

        # Reject invalid assignment
        with pytest.raises(FieldConstraintError) as exc2:
            rel.role = "manager"
        assert exc2.value.constraint == "choices"


class TestSchemaExportMapping:
    """Verifies Field metadata export mapping to JSON Schema keywords (#125)."""

    def test_to_json_schema_constraints_mapping(self) -> None:
        f = Field(
            pattern=r"^[a-z]+$",
            ge=1,
            gt=0,
            le=100,
            lt=101,
            min_length=2,
            max_length=50,
            choices=["a", "b"],
        )
        mapping = f.to_json_schema_constraints()

        assert mapping["pattern"] == r"^[a-z]+$"
        assert mapping["minimum"] == 1
        assert mapping["exclusiveMinimum"] == 0
        assert mapping["maximum"] == 100
        assert mapping["exclusiveMaximum"] == 101
        assert mapping["minLength"] == 2
        assert mapping["maxLength"] == 50
        assert mapping["enum"] == ["a", "b"]
