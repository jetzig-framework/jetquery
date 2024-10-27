const std = @import("std");

const jetcommon = @import("jetcommon");

allocator: std.mem.Allocator,
name: []const u8,
options: MigrationOptions,

const Migration = @This();

const MigrationOptions = struct {
    migrations_path: ?[]const u8 = null,
    command: ?[]const u8 = null,
};

pub fn init(allocator: std.mem.Allocator, name: []const u8, options: MigrationOptions) Migration {
    return .{ .allocator = allocator, .name = name, .options = options };
}

const Command = struct {
    command: []const u8,
    allocator: std.mem.Allocator,

    const Action = enum { create, drop, alter };

    const Modifier = enum {
        action,
        name,
        type,
        index,
        unique,
    };

    pub const DataType = enum {
        string,
        integer,
        float,
        decimal,
        boolean,
        datetime,
        text,
    };

    pub const Token = union(enum) {
        table: Table,
        column: Column,

        pub const Table = struct {
            name: ?[]const u8 = null,
            action: ?Action = null,

            pub fn writeUp(self: Table, columns: []const Column, writer: anytype) !void {
                try writer.print(
                    \\try repo.{s}Table("{s}",
                , .{
                    @tagName(self.action orelse .create),
                    self.name orelse return error.MissingTableName,
                });

                if ((self.action orelse .create) == .create) {
                    try writer.writeAll("&.{");

                    for (columns) |column| {
                        try writer.print(
                            \\t.column("{s}", .{s}, .{{
                        , .{
                            column.name orelse return error.MissingColumnName,
                            @tagName(column.type orelse .string),
                        });
                        var options_count: usize = 0;
                        inline for (comptime std.enums.values(Column.options)) |tag| {
                            if (@field(column, @tagName(tag))) |option| {
                                if (option) options_count += 1;
                            }
                        }
                        var index: usize = 0;
                        inline for (comptime std.enums.values(Column.options)) |field| {
                            if (@field(column, @tagName(field))) |option| {
                                if (option) {
                                    const sep = if (index + 1 < options_count) "," else "";
                                    try writer.print(".{s} = true{s}", .{ @tagName(field), sep });
                                    index += 1;
                                }
                            }
                        }
                        try writer.writeAll("}),");
                    }

                    try writer.writeAll("},");
                }
                try writer.writeAll(".{},);");
            }

            pub fn writeDown(self: Table, writer: anytype) !void {
                if ((self.action orelse .create) == .create) {
                    try writer.print(
                        \\try repo.dropTable("{s}", .{{}});
                        \\
                    ,
                        .{self.name orelse return error.MissingTableName},
                    );
                } else {
                    try writer.writeAll("_ = repo;");
                }
            }
        };

        pub const Column = struct {
            name: ?[]const u8 = null,
            action: ?Action = null,
            type: ?DataType = null,
            index: ?bool = null,
            unique: ?bool = null,
            not_null: ?bool = null,

            pub const options = enum { unique, not_null, index };
        };

        pub fn hasName(self: Token) bool {
            return switch (self) {
                inline else => |token| blk: {
                    if (@hasField(@TypeOf(token), "name")) {
                        break :blk token.name != null;
                    } else {
                        // If we don't have a name field, return true to allow other attributes
                        // to be assigned (name always takes precedence as we expect it as the
                        // first token after the object designator, e.g.
                        // `column:foobar:index:unique` - `foobar` is the name.
                        break :blk true;
                    }
                },
            };
        }

        pub fn hasAction(self: Token) bool {
            return switch (self) {
                inline else => |token| token.action != null,
            };
        }

        pub fn set(self: *Token, modifier: Modifier, value: anytype) void {
            switch (self.*) {
                inline else => |*token| {
                    const T = @TypeOf(token.*);
                    inline for (std.meta.fields(T)) |field| {
                        if (comptime hasField(T, field.name, @TypeOf(value))) {
                            if (std.mem.eql(u8, field.name, @tagName(modifier))) {
                                @field(token, field.name) = @field(token, field.name) orelse value;
                            }
                        } else if (comptime hasEnum(T, field.name, @TypeOf(value))) {
                            inline for (comptime std.enums.values(FieldType(T, field.name))) |tag| {
                                if (std.mem.eql(u8, @tagName(tag), value)) {
                                    @field(token, field.name) = tag;
                                }
                            }
                        }
                    }
                },
            }
        }
    };

    fn isAction(modifier: []const u8) bool {
        for (std.enums.values(Action)) |action| {
            if (std.mem.eql(u8, @tagName(action), modifier)) return true;
        }
        return false;
    }

    const TokenIterator = struct {
        arg_iterator: *std.mem.TokenIterator(u8, .any),

        pub fn next(self: TokenIterator) ?Token {
            while (self.arg_iterator.next()) |arg| {
                var modifiers_it = std.mem.tokenizeScalar(u8, arg, ':');
                var maybe_token: ?Token = null;
                while (modifiers_it.next()) |modifier| {
                    if (maybe_token) |*token| {
                        if (!token.hasAction() and isAction(modifier)) {
                            token.set(.action, modifier);
                        } else if (!token.hasName()) {
                            token.set(.name, modifier);
                        } else if (modifierToken(modifier)) |modifier_token| {
                            token.set(modifier_token, true);
                        } else if (isType(modifier)) {
                            token.set(.type, modifier);
                        } else {
                            std.log.err("Invalid migration command: {s}", .{arg});
                            unreachable;
                        }
                    } else {
                        maybe_token = if (std.mem.eql(u8, modifier, "table"))
                            Token{ .table = .{} }
                        else if (std.mem.eql(u8, modifier, "column"))
                            Token{ .column = .{} }
                        else
                            null;
                    }
                }
                return maybe_token;
            }
            return null;
        }

        fn modifierToken(modifier: []const u8) ?Modifier {
            inline for (comptime std.enums.values(Modifier)) |value| {
                if (std.mem.eql(u8, @tagName(value), modifier)) return value;
            }
            return null;
        }
    };

    pub fn write(self: Command, writer: anytype) !void {
        var arg_iterator = std.mem.tokenizeAny(u8, self.command, &std.ascii.whitespace);
        var token_iterator = TokenIterator{ .arg_iterator = &arg_iterator };

        var up_buf = std.ArrayList(u8).init(self.allocator);
        const up_writer = up_buf.writer();

        var down_buf = std.ArrayList(u8).init(self.allocator);
        const down_writer = down_buf.writer();

        var columns = std.ArrayList(Command.Token.Column).init(self.allocator);
        var maybe_table: ?Command.Token.Table = null;

        while (token_iterator.next()) |token| {
            switch (token) {
                .table => |table| {
                    maybe_table = table;
                },
                .column => |column| {
                    try columns.append(column);
                },
            }
        }

        if (maybe_table) |table| {
            try table.writeUp(columns.items, up_writer);
            try table.writeDown(down_writer);
        }
        try writer.print(migration_template, .{ up_buf.items, down_buf.items });
    }
};

const migration_template =
    \\const std = @import("std");
    \\const jetquery = @import("jetquery");
    \\const t = jetquery.schema.table;
    \\
    \\pub fn up(repo: anytype) !void {{
    \\{s}
    \\}}
    \\
    \\pub fn down(repo: anytype) !void {{
    \\{s}
    \\}}
    \\
;
const default_migration = std.fmt.comptimePrint(migration_template, .{
    \\    try repo.createTable(
    \\        "my_table",
    \\        &.{
    \\            t.primaryKey("id", .{{}}),
    \\            t.column("my_string", .string, .{{}}),
    \\            t.column("my_integer", .integer, .{{}}),
    \\            t.timestamps(.{{}}),
    \\        },
    \\        .{{}},
    \\    );
    ,
    \\    try repo.dropTable("my_table", .{{}});
});

pub fn save(self: Migration) !void {
    var dir = if (self.options.migrations_path) |path|
        try std.fs.openDirAbsolute(path, .{})
    else
        try std.fs.cwd().openDir("migrations", .{});
    defer dir.close();

    var timestamp_buf: [19]u8 = undefined;
    const prefix = try timestamp(&timestamp_buf);
    const filename = try std.mem.concat(self.allocator, u8, &.{ prefix, "_", self.name, ".zig" });
    const migration_file = try dir.createFile(filename, .{ .exclusive = true });
    defer migration_file.close();

    const writer = migration_file.writer();
    try writer.writeAll(try self.render());
}

pub fn render(self: Migration) ![]const u8 {
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var buf = std.ArrayList(u8).init(alloc);
    const writer = buf.writer();

    if (self.options.command) |cmd| {
        const command = Command{ .allocator = alloc, .command = cmd };
        try command.write(writer);
    } else {
        try writer.writeAll(default_migration);
    }
    return try jetcommon.fmt.zig(
        self.allocator,
        buf.items,
        "Found errors in generated migration.",
    );
}

fn timestamp(buf: []u8) ![]const u8 {
    const datetime = jetcommon.types.DateTime.now();
    const date = datetime.date();
    const time = datetime.time();
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();
    try writer.print(
        "{d:04}-{d:02}-{d:02}_{d:02}-{d:02}-{d:02}",
        .{ @as(u16, @intCast(date.year)), date.month, date.day, time.hour, time.min, time.sec },
    );
    return stream.getWritten();
}

inline fn FieldType(T: type, comptime name: []const u8) type {
    const tag = std.enums.nameCast(std.meta.FieldEnum(T), name);
    const F = std.meta.fieldInfo(T, tag);
    return switch (@typeInfo(F.type)) {
        .optional => |info| info.child,
        else => F,
    };
}

inline fn hasField(T: type, comptime name: []const u8, VT: type) bool {
    return @hasField(T, name) and FieldType(T, name) == VT;
}

inline fn hasEnum(T: type, comptime name: []const u8, VT: type) bool {
    if (VT != []const u8) return false;
    if (!@hasField(T, name)) return false;

    const FT = FieldType(T, name);

    return @typeInfo(FT) == .@"enum";
}

inline fn isType(name: []const u8) bool {
    inline for (comptime std.enums.values(Command.DataType)) |tag| {
        if (std.mem.eql(u8, name, @tagName(tag))) return true;
    }
    return false;
}

test "default migration" {
    const migration = Migration.init(std.testing.allocator, "test_migration", .{});

    const rendered = try migration.render();
    defer std.testing.allocator.free(rendered);

    try std.testing.expectEqualStrings(default_migration, rendered);
}

test "migration from command line: create table" {
    const command = "table:create:cats column:name:string:index:unique column:paws:integer";

    const migration = Migration.init(
        std.testing.allocator,
        "test_migration",
        .{ .command = command },
    );
    const rendered = try migration.render();
    defer std.testing.allocator.free(rendered);

    try std.testing.expectEqualStrings(
        \\const std = @import("std");
        \\const jetquery = @import("jetquery");
        \\const t = jetquery.schema.table;
        \\
        \\pub fn up(repo: anytype) !void {
        \\    try repo.createTable(
        \\        "cats",
        \\        &.{
        \\            t.column("name", .string, .{ .unique = true, .index = true }),
        \\            t.column("paws", .integer, .{}),
        \\        },
        \\        .{},
        \\    );
        \\}
        \\
        \\pub fn down(repo: anytype) !void {
        \\    try repo.dropTable("cats", .{});
        \\}
        \\
    , rendered);
}

test "migration from command line: drop table" {
    const command = "table:drop:cats";

    const migration = Migration.init(
        std.testing.allocator,
        "test_migration",
        .{ .command = command },
    );
    const rendered = try migration.render();
    defer std.testing.allocator.free(rendered);

    try std.testing.expectEqualStrings(
        \\const std = @import("std");
        \\const jetquery = @import("jetquery");
        \\const t = jetquery.schema.table;
        \\
        \\pub fn up(repo: anytype) !void {
        \\    try repo.dropTable(
        \\        "cats",
        \\        .{},
        \\    );
        \\}
        \\
        \\pub fn down(repo: anytype) !void {
        \\    _ = repo;
        \\}
        \\
    , rendered);
}

test "migration from command line: alter table" {
    const command = "table:alter:cats column:color:string";

    const migration = Migration.init(
        std.testing.allocator,
        "test_migration",
        .{ .command = command },
    );
    const rendered = try migration.render();
    defer std.testing.allocator.free(rendered);

    try std.testing.expectEqualStrings(
        \\const std = @import("std");
        \\const jetquery = @import("jetquery");
        \\const t = jetquery.schema.table;
        \\
        \\pub fn up(repo: anytype) !void {
        \\    try repo.dropTable(
        \\        "cats",
        \\        .{},
        \\    );
        \\}
        \\
        \\pub fn down(repo: anytype) !void {
        \\    _ = repo;
        \\}
        \\
    , rendered);
}
