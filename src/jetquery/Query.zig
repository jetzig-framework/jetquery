const std = @import("std");

const jetquery = @import("../jetquery.zig");

/// Available SQL statement types.
const QueryType = enum { select, update, insert, delete };

/// Create a new query by passing a table definition.
/// ```zig
/// const query = Query(Schema.Cats).init(allocator);
/// ```
pub fn Query(Table: type) type {
    return struct {
        table: Table,

        pub const Definition = Table.Definition;

        pub fn select(comptime columns: []const std.meta.FieldEnum(Table.Definition)) Statement(Table, std.meta.fields(@TypeOf(.{}))) {
            var statement: Statement(Table, std.meta.fields(@TypeOf(.{}))) = undefined;
            statement.columns = columns;
            statement.field_names = .{};
            statement.field_contexts = .{};
            statement.query_type = .select;
            statement.limit_bound = null;
            return statement;
        }

        pub fn update(args: anytype) Statement(Table, std.meta.fields(@TypeOf(args))) {
            var statement: Statement(Table, std.meta.fields(@TypeOf(args))) = undefined;
            inline for (std.meta.fields(@TypeOf(args)), 0..) |field, index| {
                const value = @field(args, field.name);
                const coerced = coerce(Table, field, value);
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = coerced.value;
                statement.coerce_errors[index] = coerced.err;
                statement.field_names[index] = field.name;
                statement.field_contexts[index] = .update;
            }
            statement.columns = &.{};
            statement.query_type = .update;
            statement.limit_bound = null;
            return statement;
        }

        pub fn insert(args: anytype) Statement(Table, std.meta.fields(@TypeOf(args))) {
            var statement: Statement(Table, std.meta.fields(@TypeOf(args))) = undefined;
            inline for (std.meta.fields(@TypeOf(args)), 0..) |field, index| {
                const value = @field(args, field.name);
                const coerced = coerce(Table, field, value);
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = coerced.value;
                statement.coerce_errors[index] = coerced.err;
                statement.field_names[index] = field.name;
                statement.field_contexts[index] = .insert;
            }
            statement.columns = &.{};
            statement.query_type = .insert;
            statement.limit_bound = null;
            return statement;
        }

        pub fn delete() Statement(Table, &.{}) {
            var statement: Statement(Table, &.{}) = undefined;
            statement.columns = &.{};
            statement.query_type = .delete;
            statement.limit_bound = null;
            statement.field_contexts = .{};
            return statement;
        }
    };
}

fn ColumnType(Table: type, comptime name: []const u8) type {
    return std.meta.FieldType(
        Table.Definition,
        std.enums.nameCast(std.meta.FieldEnum(Table.Definition), name),
    );
}

fn Fields(Table: type, comptime fields: []const std.builtin.Type.StructField) type {
    var new_fields: [fields.len]std.builtin.Type.StructField = undefined;
    for (fields, 0..) |field, index| {
        new_fields[index] = .{
            .name = std.fmt.comptimePrint("{}", .{index}),
            .type = ColumnType(Table, field.name),
            .default_value = null,
            .is_comptime = false,
            .alignment = @alignOf(ColumnType(Table, field.name)),
        };
    }
    return @Type(.{
        .@"struct" = .{
            .layout = .auto,
            .fields = &new_fields,
            .decls = &.{},
            .is_tuple = true,
        },
    });
}

fn ValueType(T: type) type {
    return switch (@typeInfo(T)) {
        .comptime_int, .int => usize,
        .float, .comptime_float => f64,
        .bool => bool,
        .pointer => []const u8, // TODO
        else => @compileError("Unsupported type: " ++ @typeName(T)),
    };
}

fn canCoerceDelegate(T: type) bool {
    return switch (@typeInfo(T)) {
        .@"struct", .@"union" => @hasDecl(T, "toJetQuery") and @typeInfo(@TypeOf(T.toJetQuery)) == .@"fn",
        .pointer => |info| @hasDecl(info.child, "toJetQuery") and @typeInfo(@TypeOf(T.toJetQuery)) == .@"fn",
        else => false,
    };
}

// Call `toJetQuery` with a given type and an allocator on the given arg field. Although
// this function expects a return value not specific to JetQuery, the intention is that
// arbitrary types can implement `toJetQuery` if the author wants them to be used with
// JetQuery, otherwise a typical Zig compile error will occur. This feature is used by
// Zmpl for converting Zmpl Values, allowing e.g. request params in Jetzig to be used as
// JetQuery whereclause/etc. params.
fn coerceDelegate(Target: type, value: anytype) CoercedValue(Target) {
    const Source = @TypeOf(value);
    if (comptime canCoerceDelegate(Source)) {
        const coerced = value.toJetQuery(Target) catch |err| {
            return .{ .err = err };
        };
        return .{ .value = coerced, .err = null };
    } else {
        @compileError("Incompatible types: `" ++ @typeName(Target) ++ "` and `" ++ @typeName(Source) ++ "`");
    }
}

fn initStatement(
    Table: type,
    C: type,
    statement: *C,
    S: type,
    self: S,
    fields: []const std.builtin.Type.StructField,
    field_context: FieldContext,
    args: anytype,
) void {
    inline for (fields, 0..) |field, index| {
        const value = self.field_values[index];
        @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = value;
        statement.field_names[index] = field.name;
        statement.field_contexts[index] = self.field_contexts[index];
    }
    inline for (std.meta.fields(@TypeOf(args)), fields.len..) |field, index| {
        const value = @field(args, field.name);
        const coerced = coerce(Table, field, value);
        @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = coerced.value;
        statement.coerce_errors[index] = coerced.err;
        statement.field_names[index] = field.name;
        statement.field_contexts[index] = field_context;
    }
    statement.columns = self.columns;
    statement.query_type = self.query_type;
    statement.limit_bound = self.limit_bound;
}

fn CoercedValue(Target: type) type {
    return struct {
        value: Target = undefined, // Never used if `err` is present
        err: ?anyerror = null,
    };
}

fn coerce(
    Table: type,
    field: std.builtin.Type.StructField,
    value: anytype,
) CoercedValue(ColumnType(Table, field.name)) {
    const T = ColumnType(Table, field.name);
    return switch (@typeInfo(field.type)) {
        .int, .comptime_int => switch (@typeInfo(T)) {
            .int => .{ .value = value },
            else => coerceDelegate(T, value),
        },
        .float, .comptime_float => switch (@typeInfo(T)) {
            .float => .{ .value = value },
            else => coerceDelegate(T, value),
        },
        .pointer => |info| switch (@typeInfo(T)) {
            .int => switch (@typeInfo(info.child)) {
                .int => switch (info.size) {
                    .Slice => coerceInt(T, value),
                    else => .{ .value = value },
                },
                else => if (comptime canCoerceDelegate(info.child))
                    coerceDelegate(T, value.*)
                else
                    coerceInt(T, value),
            },
            .float => switch (@typeInfo(info.child)) {
                .float => .{ .value = value },
                else => if (comptime canCoerceDelegate(info.child))
                    coerceDelegate(T, value.*)
                else
                    coerceFloat(T, value),
            },
            .bool => switch (@typeInfo(info.child)) {
                .bool => .{ .value = value },
                else => if (comptime canCoerceDelegate(info.child))
                    coerceDelegate(T, value.*)
                else
                    coerceBool(T, value),
            },
            .pointer => if (comptime canCoerceDelegate(info.child))
                coerceDelegate(T, value.*)
            else
                .{ .value = value }, // TODO
            else => if (comptime canCoerceDelegate(info.child))
                coerceDelegate(T, value.*)
            else
                @compileError("Incompatible types: `" ++
                    @typeName(T) ++ "` and `" ++ @typeName(info.child) ++ "`"),
        },
        else => coerceDelegate(T, value),
    };
}

fn coerceInt(T: type, value: []const u8) CoercedValue(T) {
    const coerced = std.fmt.parseInt(T, value, 10) catch |err| {
        return .{
            .err = switch (err) {
                error.InvalidCharacter, error.Overflow => error.JetQueryInvalidIntegerString,
            },
        };
    };
    return .{ .value = coerced };
}

fn coerceFloat(T: type, value: []const u8) CoercedValue(T) {
    const coerced = std.fmt.parseFloat(T, value) catch |err| {
        return .{
            .err = switch (err) {
                error.InvalidCharacter => error.JetQueryInvalidFloatString,
            },
        };
    };
    return .{ .value = coerced };
}

fn coerceBool(T: type, value: []const u8) CoercedValue(T) {
    if (value.len != 1) return .{ .err = error.JetQueryInvalidBooleanString };

    const maybe_boolean = switch (value[0]) {
        '1' => true,
        '0' => false,
        else => null,
    };

    return if (maybe_boolean) |boolean|
        .{ .value = boolean }
    else
        .{ .err = error.JetQueryInvalidBooleanString };
}

const FieldContext = enum { where, update, insert };

fn Statement(Table: type, comptime fields: []const std.builtin.Type.StructField) type {
    return struct {
        field_values: Fields(Table, fields),
        field_names: [fields.len][]const u8,
        field_contexts: [fields.len]FieldContext,
        columns: []const std.meta.FieldEnum(Table.Definition) = &.{},
        limit_bound: ?usize = null,
        query_type: QueryType,
        coerce_errors: [fields.len]?anyerror,

        pub const Definition = Table.Definition;

        const Self = @This();

        pub fn where(self: Self, args: anytype) Statement(Table, fields ++ std.meta.fields(@TypeOf(args))) {
            var statement: Statement(Table, fields ++ std.meta.fields(@TypeOf(args))) = undefined;
            initStatement(Table, @TypeOf(statement), &statement, Self, self, fields, .where, args);
            return statement;
        }

        pub fn limit(self: Self, limit_bound: usize) Self {
            return .{
                .field_values = self.field_values,
                .field_names = self.field_names,
                .query_type = self.query_type,
                .columns = self.columns,
                .field_contexts = self.field_contexts,
                .limit_bound = limit_bound,
                .coerce_errors = self.coerce_errors,
            };
        }

        pub fn toSql(self: Self, buf: []u8, adapter: jetquery.adapters.Adapter) ![]const u8 {
            var stream = std.io.fixedBufferStream(buf);
            const writer = stream.writer();
            switch (self.query_type) {
                .select => try self.renderSelect(writer, adapter),
                .insert => try self.renderInsert(writer, adapter),
                .update => try self.renderUpdate(writer, adapter),
                .delete => try self.renderDelete(writer, adapter),
            }

            return stream.getWritten();
        }

        pub fn values(self: Self) Fields(Table, fields) {
            return self.field_values;
        }

        pub fn validateValues(self: Self) !void {
            inline for (self.coerce_errors) |maybe_err| {
                if (maybe_err) |err| return err;
            }
        }

        fn fieldContext(self: Self, index: usize, context: FieldContext) bool {
            if (self.field_contexts.len == 0) return false;
            return self.field_contexts[index] == context;
        }

        // Render the query's `select` statement as SQL.
        fn renderSelect(self: Self, writer: anytype, adapter: jetquery.adapters.Adapter) !void {
            try writer.print("SELECT ", .{});
            var first = true;
            for (self.columns) |column| {
                if (!first) try writer.print(", ", .{}) else first = false;
                try writer.print("{}", .{adapter.identifier(@tagName(column))});
            }
            try writer.print(" FROM {}", .{adapter.identifier(Table.table_name)});
            try self.renderWhere(writer, adapter);
            if (self.limit_bound) |bound| try writer.print(" LIMIT {}", .{bound});
        }

        // Render the query's `insert` statement as SQL.
        fn renderInsert(self: Self, writer: anytype, adapter: jetquery.adapters.Adapter) !void {
            try writer.print("INSERT INTO {} (", .{adapter.identifier(Table.table_name)});
            var first = true;
            for (self.field_names, 0..) |field_name, index| {
                if (self.fieldContext(index, .insert)) {
                    if (!first) try writer.print(", ", .{}) else first = false;
                    try writer.print("{}", .{adapter.identifier(field_name)});
                }
            }
            try writer.print(") VALUES (", .{});
            first = true;
            inline for (self.field_names, self.field_values, 0..) |field_name, field_value, index| {
                _ = field_name; // may use later with named bind-params
                var buf: [8]u8 = undefined;
                if (self.fieldContext(index, .insert)) {
                    if (!first) try writer.print(", ", .{}) else first = false;
                    try writer.print("{s}", .{try adapter.paramSql(&buf, field_value, index)});
                }
            }
            try writer.print(")", .{});
        }

        // Render the query's `update` statement as SQL.
        fn renderUpdate(self: Self, writer: anytype, adapter: jetquery.adapters.Adapter) !void {
            try writer.print("UPDATE {} SET ", .{adapter.identifier(Table.table_name)});
            var first = true;
            inline for (self.field_names, self.field_values, 0..) |field_name, field_value, index| {
                if (self.fieldContext(index, .update)) {
                    if (!first) try writer.print(", ", .{}) else first = false;
                    var buf: [8]u8 = undefined;
                    try writer.print("{} = {s}", .{
                        adapter.identifier(field_name),
                        try adapter.paramSql(&buf, field_value, index),
                    });
                }
            }
            try self.renderWhere(writer, adapter);
        }

        // Render the query's `delete` statement as SQL.
        fn renderDelete(self: Self, writer: anytype, adapter: jetquery.adapters.Adapter) !void {
            try writer.print("DELETE FROM {}", .{adapter.identifier(Table.table_name)});
            try self.renderWhere(writer, adapter);
        }

        // Render the query's `where` clause as SQL.
        fn renderWhere(self: Self, writer: anytype, adapter: jetquery.adapters.Adapter) !void {
            var first = true;
            inline for (self.field_names, self.field_values, 0..) |field_name, field_value, index| {
                if (self.fieldContext(index, .where)) {
                    if (first) {
                        try writer.print(" WHERE ", .{});
                        first = false;
                    } else {
                        try writer.print(" AND ", .{});
                    }

                    var buf: [8]u8 = undefined;
                    try writer.print(
                        \\{} = {s}
                    , .{
                        adapter.identifier(field_name),
                        try adapter.paramSql(&buf, field_value, index),
                    });
                }
            }
        }
    };
}
