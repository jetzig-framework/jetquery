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
        context: []const ValueType,
        sql: []const u8,

        pub fn Tuple(self: @This()) type {
            comptime {
                var types: [self.context.len]type = undefined;
                for (self.context, 0..) |ctx, index| types[index] = ctx.type;
                return std.meta.Tuple(&types);
            }
        }

        pub fn values(self: @This(), args: anytype) self.Tuple() {
            var vals: self.Tuple() = undefined;
            inline for (self.context, 0..) |ctx, index| {
                const name = std.fmt.comptimePrint("{d}", .{index});
                @field(vals, name) = resolve(
                    @TypeOf(@field(vals, name)),
                    args,
                    ctx.path,
                );
            }
            return vals;
        }

        fn resolve(T: type, args: anytype, comptime path: []const u8) T {
            comptime var FT: type = @TypeOf(args);
            var field: FT = undefined;
            comptime var index: usize = 0;
            inline for (path) |char| {
                const field_name: ?[]const u8 = comptime if (char == 0) path[0..index] else null;
                if (field_name) |name| {
                    @compileLog(std.fmt.comptimePrint("field_name: {s}\n", .{name}));
                    field = @field(field, name);
                    FT = @TypeOf(field);
                }
                index += 1;
            }
            return @field(field, path[index..]);
        }
    };

    const clause: WhereClause = comptime blk: {
        const node = parseNodeComptime(@TypeOf(where), @TypeOf(where), "root", &.{});
        // debugNode(node, 0);
        var buf: [1024]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buf);
        var index: usize = 0;
        try node.render(stream.writer(), &index, 0, null);
        break :blk .{ .sql = stream.getWritten() ++ "", .context = &node.values_info() };
    };
    // inline for (clause.context) |ctx| std.debug.print("{any}\n", .{ctx.type});
    // var vals: clause.context.tuple = undefined;
    std.debug.print("**** {s}\n\n", .{clause.sql});
    std.debug.print("**** {any}\n\n", .{clause.Tuple()});
    _ = clause.values(where);
    // std.debug.print("**** {any}\n\n", .{clause.values(where)});
    // inline for (clause.context) |ctx| {
    //     std.debug.print("**** {s}\n", .{ctx.path});
    // }
    // std.debug.print("**** {any}\n\n", .{WhereClause.values(clause.context, clause.Tuple(), where)});
    // std.debug.print("{any}\n", .{context.Values});

    // var paths: [count][]const u8 = undefined;
    //
    // var tuple: [count]type = undefined;
    // for (types, 0..) |value_type, value_index| {
    //     tuple[value_index] = value_type.type;
    // }
    // .{ .paths = &paths, .tuple = std.meta.Tuple(&tuple) };
}

const Context = struct {
    tuple: type,
    paths: [][]const u8,
};

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
            else => .{ .value = .{ .name = name, .type = T, .path = path } },
        };
    }
}

const ValueType = struct {
    type: type,
    path: []const u8,
};

const Node = union(enum) {
    pub const Condition = enum { NOT, AND, OR };
    pub const Value = struct {
        name: []const u8,
        type: type,
        path: [][]const u8,

        fn joinPath(comptime self: Value) [self.pathSize()]u8 {
            comptime {
                var buf: [self.pathSize()]u8 = undefined;
                var cursor: usize = 0;
                for (self.path[0 .. self.path.len - 1]) |field_name| {
                    @memcpy(buf[cursor .. cursor + field_name.len], field_name);
                    std.mem.writeInt(
                        u8,
                        buf[cursor + field_name.len .. cursor + field_name.len + 1],
                        0,
                        native_endian,
                    );
                    cursor += field_name.len + 1;
                }
                @memcpy(buf[cursor..], self.path[self.path.len - 1]);
                return buf;
            }
        }

        fn pathSize(comptime self: Value) usize {
            comptime {
                var size: usize = 0;
                for (self.path[0 .. self.path.len - 1]) |field_name| {
                    size += field_name.len + 1;
                }
                size += self.path[self.path.len - 1].len;
                return size;
            }
        }
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

    pub fn values_info(comptime self: Node) [self.countValues()]ValueType {
        var types: [self.countValues()]ValueType = undefined;
        var index: usize = 0;

        switch (self) {
            .condition => {},
            .value => |value| {
                types[index.*] = .{ .type = value.type, .path = &value.joinPath() };
                index.* += 1;
            },
            .group => |group| {
                appendValueTypes(group, &types, &index);
            },
        }

        return types;
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

    fn appendValueTypes(comptime group: Group, comptime types: []ValueType, comptime index: *usize) void {
        for (group.children) |child| {
            switch (child) {
                .condition => {},
                .value => |value| {
                    types[index.*] = .{ .type = value.type, .path = &value.joinPath() };
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
