const std = @import("std");

const jetquery = @import("../../jetquery.zig");

const Column = @import("Column.zig");

pub fn column(
    name: []const u8,
    column_type: Column.Type,
    options: Column.Options,
) Column {
    return .{ .name = name, .type = column_type, .options = options };
}

pub const PrimaryKeyOptions = struct {
    type: Column.Type = .integer,
};

pub fn primaryKey(name: []const u8, options: PrimaryKeyOptions) Column {
    return .{
        .name = name,
        .type = options.type,
        .options = .{ .optional = false },
        .primary_key = true,
    };
}

pub const TimestampsOptions = struct {
    created_at: bool = true,
    updated_at: bool = true,

    pub fn toSql(
        self: TimestampsOptions,
        writer: anytype,
        adapter: anytype,
    ) !void {
        const created_at = comptime Column.init(
            jetquery.default_column_names.created_at,
            .datetime,
            .{},
        );
        const updated_at = comptime Column.init(
            jetquery.default_column_names.created_at,
            .datetime,
            .{},
        );
        if (self.created_at) {
            try writer.print("{s}{s}{s}", .{
                adapter.identifier(jetquery.default_column_names.created_at),
                adapter.columnTypeSql(created_at),
                adapter.notNullSql(),
            });
        }

        if (self.created_at and self.updated_at) try writer.print(", ", .{});

        if (self.updated_at) {
            try writer.print("{s}{s}{s}", .{
                adapter.identifier(jetquery.default_column_names.updated_at),
                adapter.columnTypeSql(updated_at),
                adapter.notNullSql(),
            });
        }
    }
};

pub fn timestamps(options: TimestampsOptions) Column {
    return .{ .name = undefined, .type = undefined, .options = .{}, .timestamps = options };
}
