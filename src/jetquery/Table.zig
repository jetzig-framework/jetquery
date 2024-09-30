const std = @import("std");

const jetquery = @import("../jetquery.zig");

const TableOptions = []const type;

/// Abstraction of a database table. Define a schema with:
/// ```zig
/// const Schema = struct {
///     pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
/// };
/// ```
pub fn Table(table_name: []const u8, T: type, options: anytype) type {
    return struct {
        pub const Definition = T;
        pub const name = table_name;
        pub const relations = if (@hasField(@TypeOf(options), "relations")) options.relations else .{};

        pub fn insert(repo: jetquery.Repo, args: anytype) !void {
            try repo.insert(T, args);
        }

        pub fn columns() [std.meta.fields(Definition).len]jetquery.columns.Column {
            comptime {
                const fields = std.meta.fields(Definition);
                var buf: [fields.len]jetquery.columns.Column = undefined;
                for (fields, 0..) |field, index| {
                    buf[index] = .{
                        .name = field.name,
                        .table = @This(),
                        .type = field.type,
                    };
                }
                return buf;
            }
        }

        pub fn column(comptime column_name: []const u8) jetquery.columns.Column {
            comptime {
                return for (columns()) |col| {
                    if (std.mem.eql(u8, column_name, col.name)) break col;
                } else @compileError(std.fmt.comptimePrint(
                    "No column named `{s}` defined in Schema for `{s}'",
                    .{ column_name, table_name },
                ));
            }
        }
    };
}
