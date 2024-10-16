const std = @import("std");

const Column = @import("Column.zig");

pub fn column(
    name: []const u8,
    column_type: Column.Type,
    options: Column.Options,
) Column {
    return .{ .name = name, .type = column_type, .options = options };
}

pub fn primaryKey(name: []const u8, options: struct {}) Column {
    _ = options;
    return .{ .name = name, .type = .integer, .options = .{}, .primary_key = true };
}

pub fn timestamps(options: struct {}) Column {
    _ = options;
    return .{ .name = undefined, .type = undefined, .options = .{}, .timestamps = true };
}
