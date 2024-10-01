const std = @import("std");

pub fn main() !void {
    var hercules_buf: [8]u8 = undefined;
    const hercules = try std.fmt.bufPrint(&hercules_buf, "{s}", .{"Hercules"});

    const where = .{
        .{ .NOT, .{ .foo = "bar" } },
        .{ .AND, .{ .bar = hercules } },
        .{
            .OR,
            .{ .qux = "quux" },
            .{ .OR, .{ .blah = hercules }, .{ .bloop = "blop" } },
        },
        .{
            .AND,
            .{ .qux = "quux" },
            .{ .OR, .{ .blah = hercules }, .{ .bloop = "blop" } },
        },
        .{
            .NOT,
            .{ .qux = "quux" },
            .{ .OR, .{ .blah = hercules }, .{ .bloop = "blop" } },
        },
    };

    comptime {
        const node = parseNodeComptime(@TypeOf(where), @TypeOf(where), "root", &.{});
        // _ = node;
        for (node.group.children) |child| {
            @compileLog(std.fmt.comptimePrint("{any}", .{child}));
            switch (child) {
                .group => |group| {
                    for (group.children) |gc| {
                        switch (gc) {
                            .condition => |c| @compileLog(@tagName(c)),
                            .value => |v| @compileLog(v.path),
                            else => {},
                        }
                    }
                },
                else => {},
            }
        }
    }
}

fn parseNodeComptime(OG: type, T: type, comptime name: []const u8, comptime chain: [][]const u8) Node {
    comptime {
        return switch (@typeInfo(T)) {
            .@"struct" => |info| blk: {
                var nodes: [info.fields.len]Node = undefined;
                for (info.fields, 0..) |field, index| {
                    var appended_chain: [chain.len + 1][]const u8 = undefined;
                    for (0..chain.len) |idx| appended_chain[idx] = chain[idx];
                    appended_chain[chain.len] = field.name;
                    nodes[index] = parseNodeComptime(OG, field.type, field.name, &appended_chain);
                }
                break :blk .{ .group = .{ .children = &nodes } };
            },
            .enum_literal => blk: {
                if (chain.len == 0) @compileError("oops");

                var t: type = OG;
                for (chain[0 .. chain.len - 1]) |c| {
                    t = std.meta.FieldType(t, std.enums.nameCast(std.meta.FieldEnum(t), c));
                }
                const value: t = undefined;
                const condition = @field(value, chain[chain.len - 1]);
                break :blk .{ .condition = condition };
            },
            else => .{ .value = .{ .name = name, .type = T, .path = chain } },
        };
    }
}

const Node = union(enum) {
    pub const Condition = enum { NOT, AND, OR };
    pub const Value = struct {
        name: []const u8,
        type: type,
        path: [][]const u8,
    };
    pub const Group = struct {
        children: []const Node,
    };

    condition: Condition,
    value: Value,
    group: Group,
};

fn debug(value: anytype) void {
    @compileLog(std.fmt.comptimePrint("{any}", .{value}));
}
