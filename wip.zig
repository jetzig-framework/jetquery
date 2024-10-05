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
            .OR,
            .{ .blah = hercules, .bloop = 128 },
        },
        .{
            .{ .qux = "quux" },
            .OR,
            .{ .blah = hercules, .bloop = "blop" },
            .{ .peep = .{ .boop = true, .bop = "ho" } },
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

    const node = nodeTree(@TypeOf(where), @TypeOf(where), "root", &.{});
    const context = node.context();

    std.debug.print("**** {s}\n\n", .{context.sql});
    std.debug.print("**** {any}\n\n", .{context.Tuple});
    const vals = context.values(where);
    std.debug.print("**** {any}\n\n", .{vals});
}

fn nodeTree(OG: type, T: type, comptime name: []const u8, comptime path: [][]const u8) Node {
    comptime {
        return switch (@typeInfo(T)) {
            .@"struct" => |info| blk: {
                var nodes: [info.fields.len]Node = undefined;

                for (info.fields, 0..) |field, index| {
                    var appended_path: [path.len + 1][]const u8 = undefined;
                    for (0..path.len) |idx| appended_path[idx] = path[idx];
                    appended_path[path.len] = field.name;
                    nodes[index] = nodeTree(OG, field.type, field.name, &appended_path);
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
                    const tuple_field_name = std.fmt.comptimePrint("{d}", .{index});
                    const field_type = @TypeOf(@field(values, tuple_field_name));
                    // Index evaluated at runtime so this check keeps the compiler happy:
                    if (comptime CoercedComptimeValue(@TypeOf(arg)) == field_type) {
                        @field(values, tuple_field_name) = arg;
                    } else unreachable;
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

    pub const Context = struct {
        sql: []const u8,
        Tuple: type,

        pub fn values(comptime self: Context, args: anytype) self.Tuple {
            var vals: self.Tuple = undefined;
            var index: usize = 0;
            assignValues(args, self.Tuple, &vals, &index);
            return vals;
        }
    };

    const Counter = struct {
        count: usize,

        const Self = @This();

        pub fn write(self: *Self, bytes: []const u8) !void {
            self.count += bytes.len;
        }
        pub fn print(self: *Self, comptime fmt: []const u8, comptime args: anytype) !void {
            self.write(std.fmt.comptimePrint(fmt, args)) catch unreachable;
        }
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
    ) void {
        switch (self) {
            .condition => |capture| {
                const operator = switch (capture) {
                    .NOT => if (prev == null) "NOT" else "AND NOT",
                    else => |tag| @tagName(tag),
                };
                writer.print(" {s} ", .{operator}) catch unreachable;
            },
            .value => |value| {
                value_index.* += 1;
                const is_sequence = if (prev) |capture| capture == .value else false;
                const prefix = if (is_sequence) " AND " else "";
                writer.print("{s}{s} = ${}", .{ prefix, value.name, value_index.* }) catch unreachable;
            },
            .group => |group| {
                if (group.children.len > 1) writer.print("(", .{}) catch unreachable;
                var prev_child: ?Node = null;
                for (group.children) |child| {
                    if (prev_child) |capture| {
                        if (child == .group and capture == .group) writer.print(" AND ", .{}) catch unreachable;
                    }
                    child.render(writer, value_index, depth + 1, prev_child);
                    prev_child = child;
                }
                if (group.children.len > 1) writer.print(")", .{}) catch unreachable;
            },
        }
    }

    pub fn context(comptime self: Node) Context {
        comptime {
            var counter = Counter{ .count = 0 };
            var value_index: usize = 0;
            self.render(&counter, &value_index, 0, null);

            var buf: [counter.count]u8 = undefined;
            var stream = std.io.fixedBufferStream(&buf);
            value_index = 0;
            self.render(stream.writer(), &value_index, 0, null);
            return .{ .sql = stream.getWritten() ++ "", .Tuple = self.Tuple() };
        }
    }

    fn Tuple(comptime self: Node) type {
        var types: [self.countValues()]type = undefined;
        var index: usize = 0;

        switch (self) {
            .condition => {},
            .value => |value| {
                types[index.*] = CoercedComptimeValue(value.type);
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
                    types[index.*] = CoercedComptimeValue(value.type);
                    index.* += 1;
                },
                .group => |capture| {
                    appendValueTypes(capture, types, index);
                },
            }
        }
    }
};

fn coerceComptimeValue(value: anytype) CoercedComptimeValue(@TypeOf(value)) {
    return @as(CoercedComptimeValue(@TypeOf(value)), value);
}

fn CoercedComptimeValue(T: type) type {
    return switch (@typeInfo(T)) {
        .comptime_int => usize,
        .comptime_float => f64,
        else => T,
    };
}

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
