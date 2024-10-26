const std = @import("std");

const jetquery = @import("../jetquery.zig");
const AuxiliaryQuery = @import("Query.zig").AuxiliaryQuery;

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
        // TODO: Fetch auxiliary queries and merge.
        return switch (self.*) {
            inline else => |*adapted_result| blk: {
                var row = try adapted_result.next(query) orelse break :blk null;

                self.extendInternalFields(@TypeOf(query), &row);
                break :blk row;
            },
        };
    }

    pub fn all(self: *Result, query: anytype) ![]@TypeOf(query).ResultType {
        // TODO: Eat the spaghetti
        const ResultType = @TypeOf(query).ResultType;

        return switch (self.*) {
            inline else => |*adapted_result| blk: {
                var rows = try adapted_result.all(query);
                const MergedRow = MergedRowType(query.auxiliary_queries, ResultType);
                const primary_key = @TypeOf(query).info.Table.primary_key;
                const primary_key_present = @hasField(
                    @TypeOf(query).info.Table.Definition,
                    primary_key,
                );

                var id_array = IdArray(@TypeOf(query), primary_key).init(adapted_result.allocator);
                defer id_array.deinit();

                var id_map = IdMap(@TypeOf(query), primary_key).init(adapted_result.allocator);
                defer id_map.deinit();

                var aux_map = std.AutoHashMap(usize, MergedRow).init(adapted_result.allocator);
                defer aux_map.deinit();

                for (rows, 0..) |row, index| {
                    if (comptime primary_key_present) {
                        const id = @field(row, primary_key);
                        try id_map.put(id, index);
                        try id_array.append(id);
                    }
                    var adapted_row = row;
                    self.extendInternalFields(@TypeOf(query), &adapted_row);

                    var merged_row: MergedRow = undefined;
                    inline for (query.auxiliary_queries) |init_aux_query| {
                        const aux_type = AuxType(ResultType, init_aux_query.relation);
                        @field(
                            merged_row,
                            init_aux_query.relation.relation_name,
                        ) = std.ArrayList(aux_type).init(adapted_result.allocator);
                    }
                    const aux_values = try aux_map.getOrPut(index);
                    aux_values.value_ptr.* = merged_row;
                    rows[index] = adapted_row;
                }

                const ids = id_array.items;

                inline for (query.auxiliary_queries) |aux_query| {
                    const foreign_key = comptime aux_query.relation.foreign_key orelse
                        @TypeOf(query).info.Table.defaultForeignKey();
                    const Args = comptime args_blk: {
                        var fields: [1]std.builtin.Type.StructField = .{jetquery.fields.structField(
                            foreign_key,
                            []const jetquery.fields.fieldType(
                                aux_query.relation.Source.Definition,
                                foreign_key,
                            ),
                        )};
                        break :args_blk jetquery.fields.structType(&fields);
                    };
                    var args: Args = undefined;

                    if (comptime primary_key_present) {
                        @field(args, foreign_key) = ids;
                    }

                    const q = aux_query.query.where(args);
                    // Order args are a dynamic type so we can't easily use an optional here like
                    // we can with `limit()`.
                    const q_order = if (comptime @TypeOf(aux_query.relation.order_by) != @TypeOf(null))
                        q.orderBy(aux_query.relation.order_by)
                    else
                        q;
                    const q_limit = if (aux_query.relation.limit) |limit|
                        q_order.limit(limit)
                    else
                        q_order;

                    var aux_result = try adapted_result.repo.executeInternal(
                        q_limit,
                        adapted_result.caller_info,
                    );
                    defer aux_result.deinit();

                    const aux_type = AuxType(ResultType, aux_query.relation);

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

                        if (comptime primary_key_present) {
                            const foreign_key_value = @field(aux_row, foreign_key);
                            const maybe_row_index = switch (@typeInfo(@TypeOf(foreign_key_value))) {
                                .optional => if (foreign_key_value) |value|
                                    id_map.get(value)
                                else
                                    null,
                                else => id_map.get(foreign_key_value),
                            };

                            if (maybe_row_index) |row_index| {
                                // We pre-fill the map with an empty `MergedRow` so this is
                                // guaranteed to exist (or we have a bug).
                                const aux_values = aux_map.getEntry(row_index).?;
                                try @field(
                                    aux_values.value_ptr.*,
                                    aux_query.relation.relation_name,
                                ).append(adapted_aux_row);
                            }
                        }
                    }

                    try aux_result.drain();
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

    pub inline fn duration(self: Result) i64 {
        return switch (self) {
            inline else => |adapted_result| adapted_result.duration,
        };
    }

    fn IdMap(Query: type, comptime primary_key: []const u8) type {
        const PK = if (comptime @hasField(Query.info.Table.Definition, primary_key))
            jetquery.fields.fieldType(Query.info.Table.Definition, primary_key)
        else
            void;
        return switch (PK) {
            []const u8 => std.StringHashMap(usize),
            else => std.AutoHashMap(PK, usize),
        };
    }

    fn IdArray(Query: type, comptime primary_key: []const u8) type {
        const PK = if (comptime @hasField(Query.info.Table.Definition, primary_key))
            jetquery.fields.fieldType(Query.info.Table.Definition, primary_key)
        else
            void;
        return std.ArrayList(PK);
    }
};

fn MergedRowType(auxiliary_queries: []const AuxiliaryQuery, ResultType: type) type {
    var fields: [auxiliary_queries.len]std.builtin.Type.StructField = undefined;
    for (auxiliary_queries, 0..) |aux_query, index| {
        fields[index] = jetquery.fields.structField(
            aux_query.relation.relation_name,
            std.ArrayList(AuxType(ResultType, aux_query.relation)),
        );
    }
    return jetquery.fields.structType(&fields);
}

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
