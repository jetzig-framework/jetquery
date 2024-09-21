const std = @import("std");

const jetquery = @import("../jetquery.zig");

/// Available SQL statement types.
const QueryType = enum { select, update, insert, delete };

/// Create a new query by passing a table definition.
/// ```zig
/// const query = Query(Schema.Cats).init(allocator);
/// ```
pub fn Query(T: type) type {
    return struct {
        table: T,

        pub const Definition = T.Definition;

        pub fn select(comptime columns: []const std.meta.FieldEnum(T.Definition)) Statement(.select, T, columns) {
            return Statement(.select, T, columns){ .columns = columns };
        }

        pub fn update(args: anytype) Clause(T, std.meta.fields(@TypeOf(args))) {
            var clause: Clause(T, std.meta.fields(@TypeOf(args))) = undefined;
            inline for (std.meta.fields(@TypeOf(args)), 0..) |field, index| {
                const value = @field(args, field.name);
                @field(clause.field_values, std.fmt.comptimePrint("{}", .{index})) = value;
                clause.field_names[index] = field.name;
            }
            clause.columns = &.{};
            clause.query_type = .update;
            clause.limit_bound = null;
            return clause;
        }

        pub fn insert(args: anytype) Clause(T, std.meta.fields(@TypeOf(args))) {
            var clause: Clause(T, std.meta.fields(@TypeOf(args))) = undefined;
            inline for (std.meta.fields(@TypeOf(args)), 0..) |field, index| {
                const value = @field(args, field.name);
                @field(clause.field_values, std.fmt.comptimePrint("{}", .{index})) = value;
                clause.field_names[index] = field.name;
            }
            clause.columns = &.{};
            clause.query_type = .insert;
            clause.limit_bound = null;
            return clause;
        }

        pub fn delete() Clause(T, &.{}) {
            var clause: Clause(T, &.{}) = undefined;
            clause.columns = &.{};
            clause.query_type = .delete;
            clause.limit_bound = null;
            return clause;
        }
    };
}

fn Fields(comptime fields: []const std.builtin.Type.StructField) type {
    var new_fields: [fields.len]std.builtin.Type.StructField = undefined;
    for (fields, 0..) |field, index| {
        new_fields[index] = .{
            .name = std.fmt.comptimePrint("{}", .{index}),
            .type = FieldType(field.type),
            .default_value = null,
            .is_comptime = false,
            .alignment = @alignOf(FieldType(field.type)),
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

fn FieldType(T: type) type {
    return switch (@typeInfo(T)) {
        .comptime_int, .int => usize,
        .float, .comptime_float => f64,
        .bool => bool,
        .pointer => []const u8, // TODO
        else => @compileError("Unsupported type: " ++ @typeName(T)),
    };
}

fn initClause(C: type, clause: *C, S: type, self: S, fields: []const std.builtin.Type.StructField, args: anytype) void {
    inline for (fields, 0..) |field, index| {
        const value = clause.field_values[index];
        @field(clause.field_values, std.fmt.comptimePrint("{}", .{index})) = value;
        clause.field_names[index] = field.name;
    }
    inline for (std.meta.fields(@TypeOf(args)), fields.len..) |field, index| {
        const value = @field(args, field.name);
        @field(clause.field_values, std.fmt.comptimePrint("{}", .{index})) = value;
        clause.field_names[index] = field.name;
    }
    clause.columns = self.columns;
    clause.query_type = self.query_type;
    clause.limit_bound = self.limit_bound;
    clause.where_index = fields.len;
}

fn Clause(T: type, comptime fields: []const std.builtin.Type.StructField) type {
    return struct {
        field_values: Fields(fields),
        field_names: [fields.len][]const u8,
        columns: []const std.meta.FieldEnum(T.Definition) = &.{},
        limit_bound: ?usize = null,
        query_type: QueryType,
        where_index: ?usize = null,

        pub const Definition = T.Definition;

        const Self = @This();

        pub fn where(self: Self, args: anytype) Clause(T, fields ++ std.meta.fields(@TypeOf(args))) {
            var clause: Clause(T, fields ++ std.meta.fields(@TypeOf(args))) = undefined;
            initClause(@TypeOf(clause), &clause, Self, self, fields, args);
            return clause;
        }

        pub fn limit(self: Self, limit_bound: usize) Self {
            return .{
                .field_values = self.field_values,
                .field_names = self.field_names,
                .query_type = self.query_type,
                .columns = self.columns,
                .where_index = self.where_index,
                .limit_bound = limit_bound,
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

        pub fn values(self: Self) Fields(fields) {
            return self.field_values;
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
            try writer.print("FROM {}", .{adapter.identifier(T.table_name)});
            try self.renderWhere(writer, adapter);
            if (self.limit_bound) |bound| try writer.print(" LIMIT {}", .{bound});
        }

        // Render the query's `insert` statement as SQL.
        fn renderInsert(self: Self, writer: anytype, adapter: jetquery.adapters.Adapter) !void {
            try writer.print("INSERT INTO {} (", .{adapter.identifier(T.table_name)});
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
            try writer.print("UPDATE {} SET ", .{adapter.identifier(T.table_name)});
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
            try writer.print("DELETE FROM {}", .{adapter.identifier(T.table_name)});
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
            inline for (std.meta.fields(T.Definition)) |field| {
                if (std.mem.eql(u8, field.name, name)) return field.type;
            }
            @compileError("Type `" ++ @typeName(T) ++ "` does not define field `" ++ name ++ "`");
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

fn Statement(comptime query_type: QueryType, T: type, comptime columns: []const std.meta.FieldEnum(T.Definition)) type {
    return struct {
        columns: []const std.meta.FieldEnum(T.Definition),
        query_type: QueryType = query_type,
        field_values: []struct {} = &.{},
        where_index: ?usize = null,

        pub const Definition = T.Definition;

        pub fn where(self: @This(), args: anytype) Clause(T, std.meta.fields(@TypeOf(args))) {
            _ = self;
            var clause: Clause(T, std.meta.fields(@TypeOf(args))) = undefined;
            inline for (std.meta.fields(@TypeOf(args)), 0..) |field, index| {
                const value = @field(args, field.name);
                @field(clause.field_values, std.fmt.comptimePrint("{}", .{index})) = value;
                clause.field_names[index] = field.name;
            }
            clause.columns = columns;
            clause.query_type = query_type;
            clause.limit_bound = null;
            clause.where_index = 0;
            return clause;
        }

        pub fn limit(self: @This(), limit_bound: usize) Clause(T, &.{}) {
            return .{
                .field_names = .{},
                .field_values = .{},
                .columns = columns,
                .limit_bound = limit_bound,
                .query_type = query_type,
                .where_index = self.where_index,
            };
        }

        pub fn toSql(self: @This(), buf: []u8, adapter: jetquery.adapters.Adapter) ![]const u8 {
            return try self.where(.{}).toSql(buf, adapter);
        }

        pub fn values(self: @This()) []struct {} {
            _ = self;
            return &.{};
        }
    };
}
