const std = @import("std");

pub fn main() !void {
    // var hercules_buf: [8]u8 = undefined;
    // const hercules = try std.fmt.bufPrint(&hercules_buf, "{s}", .{"Hercules"});
    //
    // const where = .{
    //     .{ .NOT, .{ .foo = "bar" } },
    //     .{ .bar = hercules },
    //     .{
    //         .OR,
    //         .{ .qux = "quux" },
    //         .{ .NOT, .{ .blah = hercules, .bloop = "blop" } },
    //     },
    //     .{
    //         .AND,
    //         .{ .qux = "quux" },
    //         .{ .OR, .{ .blah = hercules, .bloop = "blop" } },
    //         .{ .peep = .{ .boop = "hey", .bop = "ho" } },
    //     },
    //     .{
    //         .NOT,
    //         .{ .qux = "quux" },
    //         .{ .OR, .{ .blah = hercules, .bloop = "blop" } },
    //     },
    // };

    const where = .{
        .{ .foo = "bar" }, .OR,
        .{
            .{ .baz = "qux" },
            .OR,
            .{
                .plox = "plux",
                .boop = "bap",
            },
            .NOT,
            .{ .plax = "plax" },
            .AND,
            .{ .NOT, .{ .bap = "bop", .boooop = "baap" } },
        },
        .{ .abc = "xyz" },
    };

    const foo = comptime blk: {
        const node = parseNodeComptime(@TypeOf(where), @TypeOf(where), "root", &.{});
        // debugNode(node, 0);
        var buf: [1024]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buf);
        var index: usize = 0;
        // var conditions: [1024]Node.Condition = undefined;
        // conditions[0] = .AND;
        // var condition_index: usize = 0;
        try node.render(stream.writer(), &index, 0, null);
        break :blk stream.getWritten() ++ "";
    };
    std.debug.print("{s}\n", .{foo});
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

    pub fn render(
        self: Node,
        comptime writer: anytype,
        comptime value_index: *usize,
        comptime depth: usize,
        comptime prev: ?Node,
    ) !void {
        // const px = switch (self) {
        //     .group => "G",
        //     .value => "V",
        //     .condition => "",
        // };
        // if (self == .value or self == .group) try writer.print("\n" ++ px ++ (" " ** depth), .{});
        switch (self) {
            .condition => |capture| {
                const operator = switch (capture) {
                    .NOT => "AND NOT",
                    else => |tag| @tagName(tag),
                };
                try writer.print(" {s} ", .{operator});
            },
            .value => |value| {
                value_index.* += 1;
                const is_sequence = if (prev) |capture| capture == .value else false;
                const prefix = if (is_sequence) " AND " else "";
                try writer.print("{s}{s} = ${}", .{ prefix, value.name, value_index.* });
            },
            .group => |group| {
                // var value_count: usize = 0;
                // for (group.children) |child| {
                //     if (child != .condition) value_count += 1;
                // }
                // const operator = if (group_index == 0) "" else switch (conditions[condition_index.*]) {
                //     .not => "and not ",
                //     .and, .or => |tag| @tagname(tag),
                // };
                // if (value_count > 0) try writer.print(" (", .{});
                // try writer.print(" (", .{});
                try writer.print("(", .{});
                var prev_child: ?Node = null;
                for (group.children) |child| {
                    //     // if (child_index > 0 and child == .group) try writer.print(" ZAND ", .{});
                    if (prev_child) |capture| {
                        if (child == .group and capture == .group) try writer.print(" AND ", .{});
                    }
                    try child.render(writer, value_index, depth + 1, prev_child);
                    prev_child = child;
                }
                try writer.print(")", .{});
                // if (value_count > 0) try writer.print(" )", .{});
                // const suffix = switch (conditions[condition_index.*]) {
                //     .NOT => " AND ",
                //     .AND, .OR => |tag| " " ++ @tagName(tag) ++ " ",
                // };
                // if (group_index == 0) try writer.print("{s}", .{suffix});
                // condition_index.* -= 1;
            },
        }
    }
};

fn debugNode(comptime node: Node, comptime depth: usize) void {
    const indent = " " ** depth;
    switch (node) {
        .condition => |tag| {
            @compileLog(std.fmt.comptimePrint("{s}{s}", .{ indent, @tagName(tag) }));
        },
        .value => |tag| {
            @compileLog(std.fmt.comptimePrint("{s}{s}", .{ indent, tag.name }));
        },
        .group => |tag| {
            for (tag.children) |child| {
                debugNode(child, depth + 1);
            }
        },
    }
}

fn debug(value: anytype) void {
    @compileLog(std.fmt.comptimePrint("{any}", .{value}));
}
