const std = @import("std");

const jetquery = @import("../jetquery.zig");

/// A result of an executed query.
pub const Result = union(enum) {
    postgresql: jetquery.adapters.PostgresqlAdapter.Result,

    pub fn deinit(self: *Result) void {
        switch (self.*) {
            inline else => |*adapted_result| adapted_result.deinit(),
        }
    }

    pub fn drain(self: *Result) !void {
        switch (self.*) {
            inline else => |*adapted_result| try adapted_result.drain(),
        }
    }

    pub fn next(self: *Result, query: anytype) !?@TypeOf(query).ResultType {
        return switch (self.*) {
            inline else => |*adapted_result| blk: {
                var row = try adapted_result.next(query) orelse break :blk null;

                self.extendInternalFields(@TypeOf(query), &row);
                break :blk row;
            },
        };
    }

    pub fn all(self: *Result, query: anytype) ![]@TypeOf(query).ResultType {
        const RT = @TypeOf(query).ResultType;

        return switch (self.*) {
            inline else => |*adapted_result| blk: {
                var rows = try adapted_result.all(query);
                // TODO: Infer PK type
                const T = comptime t_blk: {
                    var fields: [query.auxiliary_queries.len]std.builtin.Type.StructField = undefined;
                    for (query.auxiliary_queries, 0..) |aux_query, index| {
                        fields[index] = jetquery.fields.structField(
                            aux_query.relation.relation_name,
                            std.ArrayList(AuxType(RT, aux_query.relation)),
                        );
                    }
                    break :t_blk jetquery.fields.structType(&fields);
                };

                var ids_map = std.AutoHashMap(i32, usize).init(adapted_result.allocator);
                defer ids_map.deinit();

                var ids_array = std.ArrayList(i32).init(adapted_result.allocator);
                defer ids_array.deinit();

                var aux_map = std.AutoHashMap(usize, T).init(adapted_result.allocator);
                defer aux_map.deinit();

                const primary_key = @TypeOf(query).info.Table.primary_key;
                for (rows, 0..) |row, index| {
                    if (comptime @hasField(@TypeOf(row), primary_key)) {
                        const id = @field(row, primary_key);
                        try ids_map.put(id, index);
                        try ids_array.append(id);
                    }
                    var adapted_row = row;
                    self.extendInternalFields(@TypeOf(query), &adapted_row);
                    rows[index] = adapted_row;
                }

                const ids = ids_array.items;

                inline for (query.auxiliary_queries) |aux_query| {
                    // TODO:
                    // 2. IN for multiple values
                    const foreign_key = comptime aux_query.relation.foreign_key orelse
                        @TypeOf(query).info.Table.defaultForeignKey();
                    const Args = comptime args_blk: {
                        var fields: [1]std.builtin.Type.StructField = .{jetquery.fields.structField(
                            foreign_key,
                            jetquery.fields.fieldType(
                                aux_query.relation.Source.Definition,
                                foreign_key,
                            ),
                        )};
                        break :args_blk jetquery.fields.structType(&fields);
                    };
                    var args: Args = undefined;
                    @field(args, foreign_key) = ids[0];

                    const q = aux_query.query.where(args);
                    var aux_result = try adapted_result.repo.execute(q);
                    const aux_type = AuxType(RT, aux_query.relation);

                    while (try aux_result.next(q)) |aux_row| {
                        var extended_aux_row = aux_row;
                        self.extendInternalFields(@TypeOf(q), &extended_aux_row);

                        var adapted_aux_row: aux_type = undefined;

                        inline for (std.meta.fields(aux_type)) |field| {
                            if (comptime std.mem.startsWith(u8, field.name, "__jetquery")) continue;
                            @field(adapted_aux_row, field.name) = @field(aux_row, field.name);
                            @field(
                                adapted_aux_row.__jetquery.original_values,
                                field.name,
                            ) = @field(aux_row, field.name);
                        }

                        const maybe_row_index = ids_map.get(@field(aux_row, foreign_key));

                        if (maybe_row_index) |row_index| {
                            const aux_values = try aux_map.getOrPut(row_index);
                            if (!aux_values.found_existing) {
                                var t: T = undefined;
                                inline for (query.auxiliary_queries) |init_aux_query| {
                                    @field(
                                        t,
                                        init_aux_query.relation.relation_name,
                                    ) = std.ArrayList(aux_type).init(adapted_result.allocator);
                                }
                                aux_values.value_ptr.* = t;
                            }
                            try @field(
                                aux_values.value_ptr.*,
                                aux_query.relation.relation_name,
                            ).append(adapted_aux_row);
                        }
                    }

                    try aux_result.drain();
                    defer aux_result.deinit();
                    _ = &aux_map;
                }

                var it = aux_map.iterator();
                while (it.next()) |entry| {
                    inline for (std.meta.fields(@TypeOf(entry.value_ptr.*))) |field| {
                        @field(rows[entry.key_ptr.*], field.name) = try @field(
                            entry.value_ptr.*,
                            field.name,
                        ).toOwnedSlice();
                    }
                }

                break :blk rows;
            },
        };
    }

    pub fn unary(self: *Result, T: type) !T {
        return switch (self.*) {
            inline else => |*adapted_result| try adapted_result.unary(T),
        };
    }

    /// Run a query using the result's active connection. Used for executing queries for has_many
    /// relations while processing results.
    pub fn execute(self: *Result, query: anytype) !switch (@TypeOf(query).ResultContext) {
        .one => ?@TypeOf(query).ResultType,
        .many => jetquery.Result,
        .none => void,
    } {
        return switch (self.*) {
            inline else => |*adapted_result| try adapted_result.execute(
                query.sql,
                query.field_values,
            ),
        };
    }

    fn extendInternalFields(self: *Result, Query: type, result: *Query.ResultType) void {
        result.__jetquery.id = switch (self.*) {
            inline else => |*adapted_result| adapted_result.repo.generateId(),
        };
        result.__jetquery_model = Query.info.Table;
        result.__jetquery_schema = Query.info.Schema;

        const originals = std.meta.fields(@TypeOf(result.__jetquery.original_values));

        inline for (originals) |field| {
            @field(result.__jetquery.original_values, field.name) = @field(result, field.name);
        }

        inline for (Query.relations) |relation| {
            if (comptime relation.relation_type != .belongs_to) continue;

            inline for (relation.select_columns) |select_column| {
                const relation_field = @field(result, relation.relation_name);
                const value = @field(relation_field, select_column.name);
                @field(
                    @field(result, relation.relation_name).__jetquery.original_values,
                    select_column.name,
                ) = value;
            }
        }
    }
};

fn AuxType(ResultType: type, Relation: type) type {
    const field_name = std.enums.nameCast(
        std.meta.FieldEnum(ResultType),
        Relation.relation_name,
    );
    return switch (@typeInfo(std.meta.fieldInfo(ResultType, field_name).type)) {
        .pointer => |info| info.child,
        inline else => |tag| @compileError(std.fmt.comptimePrint(
            "Expected slice for relation, found: `{s}`",
            .{@tagName(tag) ++ "`"},
        )),
    };
}

pub const AuxiliaryResult = struct {
    result: jetquery.Result,
    relation: type,
};
