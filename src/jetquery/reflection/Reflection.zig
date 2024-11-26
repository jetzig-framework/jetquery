const std = @import("std");

const jetquery = @import("../../jetquery.zig");

pub const TableInfo = struct {
    name: []const u8,
};

pub const ColumnInfo = struct {
    name: []const u8,
    table: []const u8,
    type: jetquery.schema.Column.Type,
    optional: bool,

    pub fn zigType(self: ColumnInfo, reflection: Reflection) []const u8 {
        if (self.isPrimaryKey(reflection.primary_keys)) return switch (self.type) {
            .integer => "u32",
            .bigint => "u64",
            .smallint => "u16",
            // TODO: uuid
            else => unreachable,
        };

        if (self.isForeignKey(reflection.foreign_keys)) return switch (self.type) {
            .integer => if (self.optional) "?u32" else "u32",
            .bigint => if (self.optional) "?u64" else "u64",
            .smallint => if (self.optional) "?u16" else "u16",
            // TODO: uuid
            else => unreachable,
        };

        return switch (self.type) {
            .string, .text => if (self.optional) "?[]const u8" else "[]const u8",
            .integer => if (self.optional) "?i32" else "i32",
            .float => if (self.optional) "?f32" else "f32",
            // TODO: Maybe create a Decimal type in jetcommon to wrap pg.Numeric (etc.).
            // until there's a more standardized Zig decimal type.
            .decimal => if (self.optional) "?[]const u8" else "[]const u8",
            .boolean => if (self.optional) "?bool" else "bool",
            .datetime => if (self.optional) "?jetquery.DateTime" else "jetquery.DateTime",
            .smallint => if (self.optional) "?i16" else "i16",
            .bigint => if (self.optional) "?i64" else "i64",
            .double_precision => if (self.optional) "?f64" else "f64",
        };
    }

    fn isPrimaryKey(self: ColumnInfo, primary_keys: []const PrimaryKeyInfo) bool {
        for (primary_keys) |primary_key| {
            if (std.mem.eql(u8, primary_key.table, self.table) and
                std.mem.eql(u8, primary_key.column, self.name)) return true;
        }

        return false;
    }

    fn isForeignKey(self: ColumnInfo, foreign_keys: []const ForeignKeyInfo) bool {
        for (foreign_keys) |foreign_key| {
            if (std.mem.eql(u8, foreign_key.table, self.table) and
                std.mem.eql(u8, foreign_key.column, self.name)) return true;
        }

        return false;
    }
};

pub const PrimaryKeyInfo = struct {
    table: []const u8,
    column: []const u8,
};

pub const ForeignKeyInfo = struct {
    table: []const u8,
    column: []const u8,
    foreign_table: []const u8,
    foreign_column: []const u8,
};

allocator: std.mem.Allocator,
tables: []const TableInfo,
columns: []const ColumnInfo,
primary_keys: []const PrimaryKeyInfo,
foreign_keys: []const ForeignKeyInfo,

const Reflection = @This();

pub fn deinit(self: Reflection) void {
    self.allocator.free(self.tables);
    self.allocator.free(self.columns);
    self.allocator.free(self.primary_keys);
}

pub fn tableMap(self: Reflection, allocator: std.mem.Allocator) !std.StringHashMap(TableInfo) {
    var map = std.StringHashMap(TableInfo).init(allocator);
    for (self.tables) |table| try map.put(table.name, table);
    return map;
}
