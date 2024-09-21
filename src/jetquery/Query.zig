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
            inline for (std.meta.fields(@TypeOf(.{})), 0..) |field, index| {
                const value = @field(.{}, field.name);
                const coerced = coerce(Table, field, value);
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = coerced.value;
                statement.coerce_errors[index] = coerced.err;
                statement.field_names[index] = field.name;
            }
            statement.columns = columns;
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

fn coerceJetQuery(Target: type, Source: type, value: anytype) CoercedValue(Target) {
    if (@hasDecl(Source, "toJetQuery")) {
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
    args: anytype,
) void {
    inline for (fields, 0..) |field, index| {
        const value = self.field_values[index];
        @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = value;
        statement.field_names[index] = field.name;
    }
    inline for (std.meta.fields(@TypeOf(args)), fields.len..) |field, index| {
        const value = @field(args, field.name);
        const coerced = coerce(Table, field, value);
        @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = coerced.value;
        statement.coerce_errors[index] = coerced.err;
        statement.field_names[index] = field.name;
    }
    statement.columns = self.columns;
    statement.query_type = self.query_type;
    statement.limit_bound = self.limit_bound;
    statement.where_index = fields.len;
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
            else => coerceJetQuery(T, field.type, value),
        },
        .float, .comptime_float => switch (@typeInfo(T)) {
            .float => .{ .value = value },
            else => coerceJetQuery(T, field.type, value),
        },
        .pointer => switch (@typeInfo(T)) {
            .int => coerceInt(T, value),
            .float => coerceFloat(T, value),
            .bool => coerceBool(T, value),
            .pointer => .{ .value = value }, // TODO: Ensure string
            else => coerceJetQuery(T, field.type, value),
        },
        else => coerceJetQuery(T, field.type, value),
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
    const coerced = if (std.mem.eql(u8, value, "1"))
        true
    else if (std.mem.eql(u8, value, "0"))
        false
    else
        return .{ .err = error.JetQueryInvalidBooleanString, .value = false };

    return .{ .value = coerced };
}

fn Statement(Table: type, comptime fields: []const std.builtin.Type.StructField) type {
    return struct {
        field_values: Fields(Table, fields),
        field_names: [fields.len][]const u8,
        columns: []const std.meta.FieldEnum(Table.Definition) = &.{},
        limit_bound: ?usize = null,
        query_type: QueryType,
        where_index: ?usize = null,
        coerce_errors: [fields.len]?anyerror,

        pub const Definition = Table.Definition;

        const Self = @This();

        pub fn where(self: Self, args: anytype) Statement(Table, fields ++ std.meta.fields(@TypeOf(args))) {
            var statement: Statement(Table, fields ++ std.meta.fields(@TypeOf(args))) = undefined;
            initStatement(Table, @TypeOf(statement), &statement, Self, self, fields, args);
            return statement;
        }

        pub fn limit(self: Self, limit_bound: usize) Self {
            return .{
                .field_values = self.field_values,
                .field_names = self.field_names,
                .query_type = self.query_type,
                .columns = self.columns,
                .where_index = self.where_index,
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

        // Render the query's `select` statement as SQL.
        fn renderSelect(self: Self, writer: anytype, adapter: jetquery.adapters.Adapter) !void {
            try writer.print("SELECT ", .{});
            for (self.columns, 0..) |column, index| {
                try writer.print("{}{s} ", .{
                    adapter.identifier(@tagName(column)),
                    if (index < self.columns.len - 1) "," else "",
                });
            }
            try writer.print("FROM {}", .{adapter.identifier(Table.table_name)});
            try self.renderWhere(writer, adapter);
            if (self.limit_bound) |bound| try writer.print(" LIMIT {}", .{bound});
        }

        // Render the query's `insert` statement as SQL.
        fn renderInsert(self: Self, writer: anytype, adapter: jetquery.adapters.Adapter) !void {
            try writer.print("INSERT INTO {} (", .{adapter.identifier(Table.table_name)});
            for (self.field_names, 0..) |field_name, index| {
                try writer.print("{}{s}", .{
                    adapter.identifier(field_name),
                    if (index < self.field_names.len - 1) ", " else "",
                });
            }
            try writer.print(") VALUES (", .{});
            inline for (self.field_names, self.field_values, 0..) |field_name, field_value, index| {
                _ = field_name; // may use later with named bind-params
                var buf: [8]u8 = undefined;
                try writer.print("{s}{s}", .{
                    try adapter.paramSql(&buf, field_value, index),
                    if (index + 1 < self.field_names.len) ", " else ")",
                });
            }
        }

        // Render the query's `update` statement as SQL.
        fn renderUpdate(self: Self, writer: anytype, adapter: jetquery.adapters.Adapter) !void {
            try writer.print("UPDATE {} SET ", .{adapter.identifier(Table.table_name)});
            inline for (self.field_names, self.field_values, 0..) |field_name, field_value, index| {
                if (self.where_index == null or index < self.where_index.?) {
                    var buf: [8]u8 = undefined;
                    try writer.print("{} = {s}{s}", .{
                        adapter.identifier(field_name),
                        try adapter.paramSql(&buf, field_value, index),
                        if (index < self.field_names[0 .. self.where_index orelse self.field_names.len - 1].len - 1) ", " else "",
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
            if (self.field_names.len == 0) return;
            const where_index = self.where_index orelse return;

            try writer.print(" WHERE ", .{});
            inline for (self.field_names, self.field_values, 0..) |field_name, field_value, index| {
                if (index >= where_index) {
                    var buf: [8]u8 = undefined;
                    try writer.print(
                        \\{} = {s}{s}
                    , .{
                        adapter.identifier(field_name),
                        try adapter.paramSql(&buf, field_value, index),
                        if (index - where_index + 1 < self.field_names[where_index..].len) " AND " else "",
                    });
                }
            }
        }

        // Get the type for a given field name from the table's definition struct.
        fn CoerceFieldType(comptime name: []const u8) type {
            inline for (std.meta.fields(Table.Definition)) |field| {
                if (std.mem.eql(u8, field.name, name)) return field.type;
            }
            @compileError("Type `" ++ @typeName(Table) ++ "` does not define field `" ++ name ++ "`");
        }

        // Call `toJetQuery` with a given type and an allocator on the given arg field. Although
        // this function expects a return value not specific to JetQuery, the intention is that
        // arbitrary types can implement `toJetQuery` if the author wants them to be used with
        // JetQuery, otherwise a typical Zig compile error will occur. This feature is used by
        // Zmpl for converting Zmpl Values, allowing e.g. request params in Jetzig to be used as
        // JetQuery whereclause/etc. params.
        fn delegateCoerceValue(self: Self, args: anytype, comptime field_name: []const u8) jetquery.Value {
            return switch (CoerceFieldType(field_name)) {
                []const u8 => .{
                    .string = @field(args, field_name).toJetQuery([]const u8, self.allocator),
                },
                usize => .{
                    .integer = @field(args, field_name).toJetQuery(usize, self.allocator),
                },
                f64 => .{
                    .float = @field(args, field_name).toJetQuery(f64, self.allocator),
                },
                bool => .{
                    .boolean = @field(args, field_name).toJetQuery(bool, self.allocator),
                },
                else => |C| @compileError("Unsupported schema field type `" ++ @typeName(C) ++ "` for field `" ++ field_name ++ "`"),
            };
        }

        // Try to coerce a string to the appropriate value, e.g. parse a string to an int, etc. On
        // failure, `jetquery.Value.err` is active, which will prevent the query from executing.
        // We store errors instead of returning them to allow simple method chaining
        // (`.select().where()`, etc.).
        fn coerceString(FT: type, comptime field_name: []const u8, value: []const u8) jetquery.Value {
            return switch (FT) {
                []const u8 => .{ .string = value },
                usize => parseIntegerOrError(value),
                f64 => parseFloatOrError(value),
                bool => parseBooleanOrError(value),
                else => |C| @compileError("Unsupported schema field type `" ++ @typeName(C) ++ "` for field `" ++ field_name ++ "`"),
            };
        }

        // Parse a string into a `Value.integer`, otherwise a `Value.err`.
        fn parseIntegerOrError(input: []const u8) jetquery.Value {
            const integer = std.fmt.parseInt(usize, input, 10) catch |err| {
                return .{ .err = err };
            };
            return .{ .integer = integer };
        }

        // Parse a string into a `Value.float`, otherwise a `Value.err`.
        fn parseFloatOrError(input: []const u8) jetquery.Value {
            const float = std.fmt.parseFloat(f64, input) catch |err| {
                return .{ .err = err };
            };
            return .{ .float = float };
        }

        // Parse a string into a `Value.boolean`, otherwise a `Value.err`.
        // "1" and "0" are considered as `true` and `false` respectively.
        fn parseBooleanOrError(input: []const u8) jetquery.Value {
            if (input.len != 1) return .{ .err = error.JetQueryUnrecognizedBoolean };

            const maybe_boolean = switch (input[0]) {
                '1' => true,
                '0' => false,
                else => null,
            };

            return if (maybe_boolean) |boolean|
                .{ .boolean = boolean }
            else
                .{ .err = error.JetQueryUnrecognizedBoolean };
        }
    };
}
