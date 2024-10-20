const std = @import("std");

const jetquery = @import("../../jetquery.zig");

pub const TableInfo = struct {
    name: []const u8,
};

pub const ColumnInfo = struct {
    name: []const u8,
    table: []const u8,
    type: jetquery.Column.Type,
    null: bool,

    pub fn zigType(self: ColumnInfo) []const u8 {
        return switch (self.type) {
            .string, .text => if (self.null) "?[]const u8" else "[]const u8",
            .integer => if (self.null) "?i32" else "i32",
            .float => if (self.null) "?f64" else "f64",
            .decimal => if (self.null) "?[]const u8" else "[]const u8", // TODO
            .boolean => if (self.null) "?bool" else "bool",
            .datetime => if (self.null) "?jetquery.DateTime" else "jetquery.DateTime",
        };
    }
};

allocator: std.mem.Allocator,
tables: []const TableInfo,
columns: []const ColumnInfo,

const Reflection = @This();

pub fn deinit(self: Reflection) void {
    self.allocator.free(self.tables);
    self.allocator.free(self.columns);
}
