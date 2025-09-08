const std = @import("std");
const ArrayList = std.ArrayList;
const Allocator = std.mem.Allocator;
const Writer = std.Io.Writer;
const ArenaAllocator = std.heap.ArenaAllocator;

const jetcommon = @import("jetcommon");

allocator: Allocator,
name: []const u8,
options: MigrationOptions,

const Migration = @This();

const MigrationOptions = struct {
    migrations_path: ?[]const u8 = null,
    command: ?[]const u8 = null,
};

pub fn init(allocator: Allocator, name: []const u8, options: MigrationOptions) Migration {
    return .{ .allocator = allocator, .name = name, .options = options };
}

const Command = struct {
    command: []const u8,
    allocator: Allocator,

    const Action = enum { create, drop, alter, rename };

    const Modifier = enum {
        action,
        name,
        rename,
        type,
        index,
        unique,
        reference,
        optional,
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
            rename: ?[]const u8 = null,
            action: ?Action = null,

            pub fn writeUp(self: Table, columns: []const Column, writer: *Writer) !void {
                switch (self.action orelse .create) {
                    .create => try self.writeCreateTable(columns, writer),
                    .drop => try self.writeDropTable(writer),
                    .alter => try self.writeAlterTable(columns, writer),
                    .rename => {}, // Covered by alterTable
                }
            }

            pub fn writeDown(self: Table, writer: *Writer) !void {
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

            fn writeCreateTable(self: Table, columns: []const Column, writer: *Writer) !void {
                try writer.print(
                    \\try repo.createTable("{s}",
                , .{
                    self.name orelse return error.MissingTableName,
                });

                try writer.writeAll("&.{");

                try writer.writeAll(
                    \\t.primaryKey("id", .{}),
                );
                for (columns) |column| {
                    try writeColumn(column, writer);
                }
                try writer.writeAll("t.timestamps(.{}),");
                try writer.writeAll("},");
                try writer.writeAll(".{},);");
            }

            fn writeDropTable(self: Table, writer: *Writer) !void {
                try writer.print(
                    \\try repo.dropTable("{s}", .{{}});
                , .{self.name orelse return error.MissingTableName});
            }

            fn writeAlterTable(self: Table, columns: []const Column, writer: *Writer) !void {
                try writer.print(
                    \\try repo.alterTable("{s}", .{{
                , .{self.name orelse return error.MissingTableName});

                const has_columns = for (columns) |column| {
                    switch (column.action orelse .create) {
                        .create, .drop, .rename => break true,
                        else => unreachable,
                    }
                } else false;

                if (has_columns) try writer.writeAll(".columns = .{");

                if (self.rename) |rename_table| {
                    try writer.print(
                        \\.rename = "{s}"{s}
                    , .{ rename_table, if (has_columns) "," else "" });
                }

                try writeAlterAddColumns(columns, writer);
                try writeAlterDropColumns(columns, writer);
                try writeAlterRenameColumns(columns, writer);
                if (has_columns) try writer.writeAll("},");

                try writer.writeAll("});");
            }

            fn writeAlterAddColumns(columns: []const Column, writer: *Writer) !void {
                const has_add_column = for (columns) |column| {
                    if (column.action orelse .create == .create) break true;
                } else false;

                if (has_add_column) try writer.writeAll(".add = &.{");

                for (columns) |column| {
                    if (column.action orelse .create == .create) {
                        try writeColumn(column, writer);
                    }
                }

                if (has_add_column) try writer.writeAll("},");
            }

            fn writeAlterDropColumns(columns: []const Column, writer: *Writer) !void {
                const has_drop_column = for (columns) |column| {
                    if (column.action orelse .create == .drop) break true;
                } else false;

                if (has_drop_column) try writer.writeAll(".drop = &.{");

                for (columns) |column| {
                    if (column.action orelse .create == .drop) {
                        try writer.print(
                            \\"{s},"
                        ,
                            .{column.name orelse return error.MissingColumnName},
                        );
                    }
                }

                if (has_drop_column) try writer.writeAll("},");
            }

            fn writeAlterRenameColumns(columns: []const Column, writer: *Writer) !void {
                var count: usize = 0;
                for (columns) |column| {
                    if (column.action orelse .create == .rename) {
                        try writer.print(
                            \\.rename = .{{ .from = "{s}", .to = "{s}" }},
                        ,
                            .{
                                column.name orelse return error.MissingColumnName,
                                column.rename orelse return error.MissingColumnName,
                            },
                        );
                        count += 1;
                    }
                }

                if (count > 1) {
                    std.log.err(
                        "Multiple column renames are not permitted. " ++
                            "Create separate migrations to rename multiple columns.",
                        .{},
                    );
                }
            }

            fn writeColumn(column: Column, writer: *Writer) !void {
                var column_type: DataType = undefined;
                if (column.type) |t| {
                    column_type = t;
                } else if (column.reference_column != null) {
                    column_type = .integer;
                } else {
                    column_type = .string;
                }

                try writer.print(
                    \\t.column("{s}", .{s}, .{{
                , .{
                    column.name orelse return error.MissingColumnName,
                    @tagName(column_type),
                });
                var options_count: usize = 0;
                inline for (comptime std.enums.values(Column.options)) |tag| {
                    if (tag != .default) {
                        if (@field(column, @tagName(tag))) |option| {
                            if (option) options_count += 1;
                        }
                    } else if (column.default != null) {
                        options_count += 1;
                    }
                }

                if (column.reference_column) |_| options_count += 1;

                var index: usize = 0;
                inline for (comptime std.enums.values(Column.options)) |field| {
                    if (field != .default) {
                        if (@field(column, @tagName(field))) |option| {
                            if (option) {
                                const sep = if (index + 1 < options_count) "," else "";
                                try writer.print(".{s} = true{s}", .{ @tagName(field), sep });
                                index += 1;
                            }
                        }
                    } else if (column.default) |default_value| {
                        const sep = if (index + 1 < options_count) "," else "";
                        try writer.print(".default = \"{s}\"{s}", .{ default_value, sep });
                        index += 1;
                    }
                }

                if (try column.referenceInfo()) |reference_info| {
                    try writer.print(
                        \\.reference = .{{"{s}", "{s}"}}
                    , .{ reference_info[0], reference_info[1] });
                }

                try writer.writeAll("}),");
            }
        };

        pub const Column = struct {
            name: ?[]const u8 = null,
            rename: ?[]const u8 = null,
            action: ?Action = null,
            type: ?DataType = null,
            index: ?bool = null,
            unique: ?bool = null,
            optional: ?bool = null,
            reference: ?bool = null,
            reference_column: ?[]const u8 = null,
            default: ?[]const u8 = null,

            pub const options = enum { unique, optional, index, default };

            pub fn referenceInfo(self: Column) !?[2][]const u8 {
                if (self.reference_column == null) return null;

                var info: [2][]const u8 = undefined;
                var it = std.mem.tokenizeScalar(u8, self.reference_column.?, '.');
                var index: usize = 0;
                while (it.next()) |identifier| : (index += 1) {
                    std.debug.assert(index < 2);
                    info[index] = identifier;
                }
                std.debug.assert(index == 2);
                return info;
            }
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

        pub fn isRename(self: Token) bool {
            return switch (self) {
                inline else => |token| token.action orelse .create == .rename,
            };
        }

        pub fn hasRename(self: Token) bool {
            return switch (self) {
                inline else => |token| token.rename != null,
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

        pub fn next(self: TokenIterator) !?Token {
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
                        } else if (token.isRename() and !token.hasRename()) {
                            token.set(.rename, modifier);
                        } else if (token.* == .column and token.*.column.reference == true) {
                            token.column.reference_column = modifier;
                        } else if (token.* == .column and std.mem.eql(u8, modifier, "default")) {
                            token.column.default = ""; // Set it to empty string to mark it needs a value
                        } else if (token.* == .column and token.column.default != null and token.column.default.?.len == 0) {
                            token.column.default = modifier;
                        } else {
                            return error.InvalidMigrationCommand;
                        }
                    } else {
                        maybe_token = if (std.mem.eql(u8, modifier, "table"))
                            Token{ .table = .{} }
                        else if (std.mem.eql(u8, modifier, "column"))
                            Token{ .column = .{} }
                        else if (std.mem.eql(u8, modifier, "rename"))
                            Token{ .table = .{ .action = .alter, .rename = modifier } }
                        else {
                            return error.InvalidMigrationCommand;
                        };
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

    pub fn write(self: Command, allocator: Allocator, writer: *Writer) !void {
        var arg_iterator = std.mem.tokenizeAny(
            u8,
            self.command,
            &std.ascii.whitespace,
        );

        var token_iterator = TokenIterator{ .arg_iterator = &arg_iterator };

        var alloc_up: Writer.Allocating = .init(allocator);
        defer alloc_up.deinit();

        var alloc_down: Writer.Allocating = .init(allocator);
        defer alloc_down.deinit();

        var columns: ArrayList(Command.Token.Column) = .empty;
        defer columns.deinit(allocator);

        var maybe_table: ?Command.Token.Table = null;

        while (try token_iterator.next()) |token| {
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
            try table.writeUp(columns.items, &alloc_up.writer);
            try table.writeDown(&alloc_down.writer);
        }
        try writer.print(
            migration_template,
            .{ try alloc_up.toOwnedSlice(), try alloc_down.toOwnedSlice() },
        );
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
    \\    // The `up` function runs when a migration is applied.
    \\    //
    \\    // This example migration creates a table named `my_table` with the following columns:
    \\    // * `id`
    \\    // * `my_string`
    \\    // * `my_integer`
    \\    // * `created_at`
    \\    // * `updated_at`
    \\    //
    \\    // When present, `created_at` and `updated_at` are automatically populated by JetQuery
    \\    // when a record is created/modified.
    \\    //
    \\    // See https://www.jetzig.dev/documentation/sections/database/migrations for more details.
    \\    //
    \\    // Run `jetzig database migrate` to apply migrations.
    \\    //
    \\    // Then run `jetzig database reflect` to auto-generate `src/app/database/Schema.zig`
    \\    // (or manually edit the Schema to include your new table).
    \\    //
    \\    try repo.createTable(
    \\        "my_table",
    \\        &.{
    \\            t.primaryKey("id", .{}),
    \\            t.column("my_string", .string, .{}),
    \\            t.column("my_integer", .integer, .{}),
    \\            t.timestamps(.{}),
    \\        },
    \\        .{},
    \\    );
    ,
    \\    // The `down` function runs when a migration is rolled back.
    \\    // In this case, we drop our example table `my_table`.
    \\    //
    \\    // Run `jetzig database rollback` to roll back a migration.
    \\    //
    \\    try repo.dropTable("my_table", .{});
});

pub fn save(self: Migration, allocator: Allocator) ![]const u8 {
    const content = try self.render(allocator);

    var dir = if (self.options.migrations_path) |path|
        try std.fs.openDirAbsolute(path, .{})
    else
        try std.fs.cwd().openDir("migrations", .{});
    defer dir.close();

    var timestamp_buf: [19]u8 = undefined;
    const prefix = try timestamp(&timestamp_buf);
    const filename = try std.mem.concat(allocator, u8, &.{ prefix, "_", self.name, ".zig" });
    const migration_file = try dir.createFile(filename, .{ .exclusive = true });
    defer migration_file.close();

    const writer = migration_file.writer(.{});
    try writer.interface.writeAll(content);
    const realpath = try dir.realpathAlloc(allocator, filename);

    return realpath;
}

pub fn render(self: Migration, allocator: Allocator) ![]const u8 {
    var arena: ArenaAllocator = .init(allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var aw: Writer.Allocating = try .init(allocator);
    defer aw.deinit();

    if (self.options.command) |cmd| {
        const command = Command{ .allocator = alloc, .command = cmd };
        try command.write(allocator, &aw.writer);
    } else try aw.writer.writeAll(default_migration);
    return try jetcommon.fmt.zig(
        allocator,
        try aw.toOwnedSlice(),
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
        // TODO: Fix jetcommon types to expose these directly
        .{ @as(u16, @intCast(date.zul_date.year)), date.zul_date.month, date.zul_date.day, time.zul_time.hour, time.zul_time.min, time.zul_time.sec },
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
    const migration: Migration = .init(std.testing.allocator, "test_migration", .{});

    const rendered = try migration.render(std.testing.allocator);
    defer std.testing.allocator.free(rendered);

    try std.testing.expectEqualStrings(default_migration, rendered);
}

test "migration from command line: create table" {
    const command = "table:create:cats column:name:string:index:unique column:paws:integer:default:4 column:bio:text:default:friendly_cat column:is_active:boolean:default:true column:no_default:string:optional column:human_id:index:optional:reference:humans.id";

    const migration: Migration = .init(
        std.testing.allocator,
        "test_migration",
        .{ .command = command },
    );
    const rendered = try migration.render(std.testing.allocator);
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
        \\            t.primaryKey("id", .{}),
        \\            t.column("name", .string, .{ .unique = true, .index = true }),
        \\            t.column("paws", .integer, .{ .default = "4" }),
        \\            t.column("bio", .text, .{ .default = "friendly_cat" }),
        \\            t.column("is_active", .boolean, .{ .default = "true" }),
        \\            t.column("no_default", .string, .{ .optional = true }),
        \\            t.column("human_id", .integer, .{ .optional = true, .index = true, .reference = .{ "humans", "id" } }),
        \\            t.timestamps(.{}),
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

    // Verify that no DEFAULT clause appears for the no_default field
    try std.testing.expect(!std.mem.containsAtLeast(u8, rendered, 1, "\"no_default\", .string, .{ .optional = true, .default"));
}

test "migration from command line: create table with default values of various types" {
    const command = "table:create:defaults_test column:name:string:default:John column:count:integer:default:42 column:active:boolean:default:true column:no_default:string:optional column:price:decimal:default:19.99 column:last_update:datetime:default:now()";

    const migration: Migration = .init(
        std.testing.allocator,
        "test_defaults",
        .{ .command = command },
    );
    const rendered = try migration.render(std.testing.allocator);
    defer std.testing.allocator.free(rendered);

    try std.testing.expectEqualStrings(
        \\const std = @import("std");
        \\const jetquery = @import("jetquery");
        \\const t = jetquery.schema.table;
        \\
        \\pub fn up(repo: anytype) !void {
        \\    try repo.createTable(
        \\        "defaults_test",
        \\        &.{
        \\            t.primaryKey("id", .{}),
        \\            t.column("name", .string, .{ .default = "John" }),
        \\            t.column("count", .integer, .{ .default = "42" }),
        \\            t.column("active", .boolean, .{ .default = "true" }),
        \\            t.column("no_default", .string, .{ .optional = true }),
        \\            t.column("price", .decimal, .{ .default = "19.99" }),
        \\            t.column("last_update", .datetime, .{ .default = "now()" }),
        \\            t.timestamps(.{}),
        \\        },
        \\        .{},
        \\    );
        \\}
        \\
        \\pub fn down(repo: anytype) !void {
        \\    try repo.dropTable("defaults_test", .{});
        \\}
        \\
    , rendered);
}

test "migration from command line: drop table" {
    const command = "table:drop:cats";

    const migration: Migration = .init(
        std.testing.allocator,
        "test_migration",
        .{ .command = command },
    );
    const rendered = try migration.render(std.testing.allocator);
    defer std.testing.allocator.free(rendered);

    try std.testing.expectEqualStrings(
        \\const std = @import("std");
        \\const jetquery = @import("jetquery");
        \\const t = jetquery.schema.table;
        \\
        \\pub fn up(repo: anytype) !void {
        \\    try repo.dropTable("cats", .{});
        \\}
        \\
        \\pub fn down(repo: anytype) !void {
        \\    _ = repo;
        \\}
        \\
    , rendered);
}

test "migration from command line: alter table" {
    // XXX: This is an incoherent migration (renaming table while adding columns not permitted)
    // but it tests a lot of variations all in one command. We let the database fail if the
    // migration is not coherent.
    const command = "table:alter:cats column:color:string:index:unique column:rename:paws:feet column:drop:name rename:dogs";

    const migration: Migration = .init(
        std.testing.allocator,
        "test_migration",
        .{ .command = command },
    );
    const rendered = try migration.render(std.testing.allocator);
    defer std.testing.allocator.free(rendered);

    try std.testing.expectEqualStrings(
        \\const std = @import("std");
        \\const jetquery = @import("jetquery");
        \\const t = jetquery.schema.table;
        \\
        \\pub fn up(repo: anytype) !void {
        \\    try repo.alterTable("dogs", .{
        \\        .columns = .{
        \\            .rename = "rename",
        \\            .add = &.{
        \\                t.column("color", .string, .{ .unique = true, .index = true }),
        \\            },
        \\            .drop = &.{"name,"},
        \\            .rename = .{ .from = "paws", .to = "feet" },
        \\        },
        \\    });
        \\}
        \\
        \\pub fn down(repo: anytype) !void {
        \\    _ = repo;
        \\}
        \\
    , rendered);
}
