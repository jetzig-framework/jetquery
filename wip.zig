const std = @import("std");
const native_endian = @import("builtin").cpu.arch.endian();

pub fn main() !void {
    var hercules_buf: [8]u8 = undefined;
    const hercules = try std.fmt.bufPrint(&hercules_buf, "{s}", .{"Hercules"});

    const where = .{
        .NOT,
        .{ .foo = "bar" },
        .{ .bar = hercules },
        .OR,
        .{
            .{ .qux = "quux" },
            .{ .OR, .{ .blah = hercules, .bloop = "blop" } },
        },
        .{
            .{ .qux = "quux" },
            .{ .OR, .{ .blah = hercules, .bloop = "blop" } },
            .{ .peep = .{ .boop = "hey", .bop = "ho" } },
        },
        .NOT,
        .{
            .{ .qux = "quux" },
            .OR,
            .{ .blah = hercules, .bloop = "blop" },
        },
    };

    // const where = .{
    //     .{ .foo = "bar" },
    //     .OR,
    //     .{
    //         .{ .baz = "qux" },
    //         .OR,
    //         .{
    //             .plox = "plux",
    //             .boop = "bap",
    //         },
    //         .NOT,
    //         .{ .plax = "plax" },
    //         .NOT,
    //         .{ .bap = "bop", .boooop = "baap" },
    //     },
    //     .{ .abc = "xyz" },
    // };

    const WhereClause = struct {
        Tuple: type,
        sql: []const u8,

        pub fn values(self: @This(), args: anytype) self.Tuple {
            var vals: self.Tuple = undefined;
            var index: usize = 0;
            assignValues(args, self.Tuple, &vals, &index);
            return vals;
        }
    };

    const clause: WhereClause = comptime blk: {
        const node = parseNodeComptime(@TypeOf(where), @TypeOf(where), "root", &.{});
        // debugNode(node, 0);
        var buf: [1024]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buf);
        var index: usize = 0;
        try node.render(stream.writer(), &index, 0, null);
        break :blk .{ .sql = stream.getWritten() ++ "", .Tuple = node.Tuple() };
    };

    std.debug.print("**** {s}\n\n", .{clause.sql});
    std.debug.print("**** {any}\n\n", .{clause.Tuple});
    const vals = clause.values(where);
    std.debug.print("**** {any}\n\n", .{vals});
}

fn parseNodeComptime(OG: type, T: type, comptime name: []const u8, comptime path: [][]const u8) Node {
    comptime {
        return switch (@typeInfo(T)) {
            .@"struct" => |info| blk: {
                var nodes: [info.fields.len]Node = undefined;

                for (info.fields, 0..) |field, index| {
                    var appended_path: [path.len + 1][]const u8 = undefined;
                    for (0..path.len) |idx| appended_path[idx] = path[idx];
                    appended_path[path.len] = field.name;
                    nodes[index] = parseNodeComptime(OG, field.type, field.name, &appended_path);
                }
                break :blk .{ .group = .{ .children = &nodes } };
            },
            .enum_literal => blk: {
                if (path.len == 0) @compileError("oops");

                var t: type = OG;
                for (path[0 .. path.len - 1]) |c| {
                    t = std.meta.FieldType(t, std.enums.nameCast(std.meta.FieldEnum(t), c));
                }
                const value: t = undefined;
                const condition = @field(value, path[path.len - 1]);
                break :blk .{ .condition = condition };
            },
            else => .{ .value = .{ .name = name, .type = T } },
        };
    }
}

fn assignValues(arg: anytype, Tuple: type, values: *Tuple, values_index: *usize) void {
    return switch (@typeInfo(@TypeOf(arg))) {
        .@"struct" => |info| {
            inline for (info.fields) |field| {
                assignValues(@field(arg, field.name), Tuple, values, values_index);
            }
        },
        .enum_literal => {},
        else => {
            inline for (values, 0..) |_, index| {
                if (index == values_index.*) {
                    const is_eql_type = @TypeOf(arg) == @TypeOf(@field(
                        values,
                        std.fmt.comptimePrint("{d}", .{index}),
                    ));
                    // Index evaluated at runtime so this check keeps the compiler happy:
                    if (comptime is_eql_type) {
                        @field(values, std.fmt.comptimePrint("{d}", .{index})) = arg;
                    }
                }
            }
            values_index.* += 1;
        },
    };
}

const Node = union(enum) {
    pub const Condition = enum { NOT, AND, OR };
    pub const Value = struct {
        name: []const u8,
        type: type,
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
        switch (self) {
            .condition => |capture| {
                const operator = switch (capture) {
                    .NOT => if (prev == null) "NOT" else "AND NOT",
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
                if (group.children.len > 1) try writer.print("(", .{});
                var prev_child: ?Node = null;
                for (group.children) |child| {
                    if (prev_child) |capture| {
                        if (child == .group and capture == .group) try writer.print(" AND ", .{});
                    }
                    try child.render(writer, value_index, depth + 1, prev_child);
                    prev_child = child;
                }
                if (group.children.len > 1) try writer.print(")", .{});
            },
        }
    }

    pub fn Tuple(comptime self: Node) type {
        var types: [self.countValues()]type = undefined;
        var index: usize = 0;

        switch (self) {
            .condition => {},
            .value => |value| {
                types[index.*] = value.type;
                index.* += 1;
            },
            .group => |group| {
                appendValueTypes(group, &types, &index);
            },
        }

        return std.meta.Tuple(&types);
    }

    fn countValues(comptime self: Node) usize {
        var count: usize = 0;

        switch (self) {
            .condition => {},
            .value => {
                count += 1;
            },
            .group => |group| {
                countGroupValues(group, &count);
            },
        }

        return count;
    }

    fn countGroupValues(comptime group: Group, comptime count: *usize) void {
        for (group.children) |child| {
            switch (child) {
                .condition => {},
                .value => {
                    count.* += 1;
                },
                .group => |capture| {
                    countGroupValues(capture, count);
                },
            }
        }
    }

    fn appendValueTypes(comptime group: Group, comptime types: []type, comptime index: *usize) void {
        for (group.children) |child| {
            switch (child) {
                .condition => {},
                .value => |value| {
                    types[index.*] = value.type;
                    index.* += 1;
                },
                .group => |capture| {
                    appendValueTypes(capture, types, index);
                },
            }
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
