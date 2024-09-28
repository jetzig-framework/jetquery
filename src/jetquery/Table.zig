const std = @import("std");

const jetquery = @import("../jetquery.zig");

const TableOptions = []const type;

/// Abstraction of a database table. Define a schema with:
/// ```zig
/// const Schema = struct {
///     pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
/// };
/// ```
pub fn Table(name: []const u8, T: type, options: anytype) type {
    return struct {
        pub const Definition = T;
        pub const table_name = name;
        pub const relations = if (@hasField(@TypeOf(options), "relations")) options.relations else .{};

        pub fn insert(repo: jetquery.Repo, args: anytype) !void {
            try repo.insert(T, args);
        }

        pub fn columns() []const std.meta.FieldEnum(Definition) {
            return std.enums.values(std.meta.FieldEnum(Definition));
        }
    };
}
