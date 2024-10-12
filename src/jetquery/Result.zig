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
        const RT = @TypeOf(query).ResultType;

        return switch (self.*) {
            inline else => |*adapted_result| blk: {
                var row = try adapted_result.next(query) orelse break :blk null;

                try adapted_result.drain(); // TODO - only drain for `.one`/`.none`

                inline for (query.auxiliary_queries) |aux_query| {
                    const q = aux_query.query.where(.{ .id = row.id });
                    var aux_result = try adapted_result.execute(q.sql, q.field_values);
                    const aux_type = AuxType(RT, aux_query.relation);
                    var aux_rows = std.ArrayList(aux_type).init(adapted_result.allocator);

                    while (try aux_result.next(q)) |aux_row| {
                        var adapted_aux_row: aux_type = undefined;
                        inline for (std.meta.fields(aux_type)) |field| {
                            if (comptime std.mem.startsWith(u8, field.name, "__jetquery")) continue;
                            @field(adapted_aux_row, field.name) = @field(aux_row, field.name);
                            @field(
                                adapted_aux_row.__jetquery.original_values,
                                field.name,
                            ) = @field(aux_row, field.name);
                        }
                        try aux_rows.append(adapted_aux_row);
                    }

                    @field(row, aux_query.relation.relation_name) = try aux_rows.toOwnedSlice();
                    defer aux_result.deinit();
                }

                self.extendInternalFields(@TypeOf(query), &row);
                break :blk row;
            },
        };
    }

    pub fn all(self: *Result, query: anytype) ![]const @TypeOf(query).ResultType {
        return switch (self.*) {
            inline else => |*adapted_result| try adapted_result.all(query),
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
