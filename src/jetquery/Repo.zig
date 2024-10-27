const std = @import("std");

const jetquery = @import("../jetquery.zig");

pub fn Repo(adapter_name: jetquery.adapters.Name) type {
    return struct {
        const AdaptedRepo = @This();

        comptime adapter_name: jetquery.adapters.Name = adapter_name,
        allocator: std.mem.Allocator,
        adapter: jetquery.adapters.Adapter(adapter_name),
        result_id: std.atomic.Value(i128) = std.atomic.Value(i128).init(0),
        eventCallback: *const fn (event: jetquery.events.Event) anyerror!void = jetquery.events.defaultCallback,
        connection: ?Connection = null,
        result: ?jetquery.Result = null,

        pub const InitOptions = switch (adapter_name) {
            .postgresql => struct {
                adapter: jetquery.adapters.PostgresqlAdapter.Options,
                eventCallback: *const fn (event: jetquery.events.Event) anyerror!void = jetquery.events.defaultCallback,
                lazy_connect: bool = false,
            },
            .null => null,
        };

        pub const CreateTableOptions = struct { if_not_exists: bool = false };
        pub const DropTableOptions = struct { if_exists: bool = false };
        pub const DropDatabaseOptions = struct { if_exists: bool = false };

        /// Initialize a new Repo for executing queries.
        pub fn init(allocator: std.mem.Allocator, options: InitOptions) !AdaptedRepo {
            var env = try std.process.getEnvMap(allocator);
            defer env.deinit();

            return switch (comptime adapter_name) {
                .postgresql => .{
                    .allocator = allocator,
                    .eventCallback = options.eventCallback,
                    .adapter = .{
                        .postgresql = try jetquery.adapters.PostgresqlAdapter.init(
                            allocator,
                            try applyDefaultOptions(@TypeOf(options.adapter), options.adapter, env),
                            options.lazy_connect,
                        ),
                    },
                },
                .null => .{
                    .allococator = allocator,
                    .adapter = .{ .null = jetquery.adapters.NullAdapter{} },
                },
            };
        }

        pub fn applyDefaultOptions(T: type, initial: T, env: std.process.EnvMap) !T {
            var options: T = undefined;
            const prefix = "JETQUERY_";
            inline for (std.meta.fields(T)) |field| {
                const env_name = comptime blk: {
                    var buf: [prefix.len + field.name.len]u8 = undefined;
                    @memcpy(buf[0..prefix.len], prefix);
                    for (field.name, prefix.len..) |char, index| buf[index] = std.ascii.toUpper(char);
                    break :blk buf;
                };
                @field(options, field.name) = @field(initial, field.name) orelse switch (field.type) {
                    ?u16, ?u32 => if (env.get(&env_name)) |value|
                        try std.fmt.parseInt(@typeInfo(field.type).optional.child, value, 10)
                    else
                        comptime T.defaultValue(field.type, field.name),
                    inline else => env.get(&env_name) orelse comptime T.defaultValue(field.type, field.name),
                };
            }
            return options;
        }

        const GlobalOptions = struct {
            eventCallback: *const fn (event: jetquery.events.Event) anyerror!void = jetquery.events.defaultCallback,
            lazy_connect: bool = false,
        };

        /// Initialize a new repo using a config file. Config file build path is configured by build
        /// option `jetquery_config_path`.
        pub fn loadConfig(allocator: std.mem.Allocator, global_options: GlobalOptions) !AdaptedRepo {
            const AdapterOptions = switch (jetquery.adapter) {
                .postgresql => jetquery.adapters.PostgresqlAdapter.Options,
                .null => jetquery.adapters.NullAdapter.Options,
            };
            var options: AdapterOptions = undefined;
            inline for (std.meta.fields(AdapterOptions)) |field| {
                if (@hasField(@TypeOf(jetquery.config.database), field.name)) {
                    @field(options, field.name) = @field(jetquery.config.database, field.name);
                } else if (field.default_value) |default| {
                    const option: *field.type = @ptrCast(@alignCast(@constCast(default)));
                    @field(options, field.name) = option.*;
                } else {
                    @compileError("Missing database configuration value for: `" ++ field.name ++ "`");
                }
            }
            var init_options: InitOptions = switch (jetquery.adapter) {
                .postgresql => .{ .adapter = options },
                .null => .{ .adapter = .null },
            };

            inline for (std.meta.fields(GlobalOptions)) |field| {
                @field(init_options, field.name) = @field(global_options, field.name);
            }
            return AdaptedRepo.init(allocator, init_options);
        }

        /// Close connections and free resources.
        pub fn deinit(self: *AdaptedRepo) void {
            self.release();
            switch (self.adapter) {
                inline else => |*adapter| adapter.deinit(),
            }
        }

        /// Execute the given query and return results.
        pub fn execute(self: *AdaptedRepo, query: anytype) !switch (@TypeOf(query).ResultContext) {
            .one => ?@TypeOf(query).ResultType,
            .many => jetquery.Result,
            .none => void,
        } {
            const caller_info = try jetquery.debug.getCallerInfo(@returnAddress());
            defer if (caller_info) |info| info.deinit();

            return try self.executeInternal(query, caller_info);
        }

        /// Execute SQL with the active adapter and return a result (same as `execute` but accepts an SQL
        /// string and values instead of a generated query).
        pub fn executeSql(self: *AdaptedRepo, sql: []const u8, values: anytype) !jetquery.Result {
            const connection = try self.connectManaged();
            const caller_info = try jetquery.debug.getCallerInfo(@returnAddress());
            return try connection.executeSql(sql, values, caller_info);
        }

        pub const Connection = union(enum) {
            postgresql: jetquery.adapters.PostgresqlAdapter.Connection,

            pub fn execute(
                self: Connection,
                query: anytype,
                caller_info: ?jetquery.debug.CallerInfo,
            ) !switch (@TypeOf(query).ResultContext) {
                .one => ?@TypeOf(query).ResultType,
                .many => jetquery.Result,
                .none => void,
            } {
                return switch (self) {
                    .postgresql => |*connection| result_blk: {
                        try query.validateValues();
                        try query.validateDelete();
                        var result = try connection.execute(query.sql, query.field_values, caller_info);
                        break :result_blk switch (@TypeOf(query).ResultContext) {
                            .one => blk: {
                                // TODO: Create a new ResultContext `.unary` instead of hacking it in here.
                                if (query.query_context == .count) {
                                    defer result.deinit();
                                    const unary = try result.unary(@TypeOf(query).ResultType);
                                    try result.drain();
                                    return unary;
                                }
                                const row = try result.next(query);
                                defer result.deinit();
                                try result.drain();
                                break :blk row;
                            },
                            .many => result,
                            .none => blk: {
                                try result.drain();
                                defer result.deinit();
                                break :blk {};
                            },
                        };
                    },
                };
            }

            /// Execute SQL with the active adapter without returning a result.
            pub fn executeVoid(
                self: Connection,
                sql: []const u8,
                values: anytype,
                caller_info: ?jetquery.debug.CallerInfo,
            ) !void {
                var result = switch (self) {
                    inline else => |connection| try connection.execute(sql, values, caller_info),
                };
                try result.drain();
                result.deinit();
            }

            /// Execute SQL with the active adapter and return a result (same as `execute` but accepts an
            /// SQL string and values instead of a generated query).
            pub fn executeSql(
                self: Connection,
                sql: []const u8,
                values: anytype,
                caller_info: ?jetquery.debug.CallerInfo,
            ) !jetquery.Result {
                return switch (self) {
                    inline else => |connection| try connection.execute(sql, values, caller_info),
                };
            }

            /// Release connection to the pool.
            pub fn release(self: Connection) void {
                switch (self) {
                    inline else => |connection| connection.release(),
                }
            }
        };

        /// Establish a new connection. Caller is responsible for calling `connection.release()` when
        /// connection can be returned to the pool. Only use this function if a new connection is
        /// specifically required, otherwise use `repo.execute()` which will automatically assign a
        /// connection and release it on `repo.deinit()`.
        ///
        /// The returned `Connection` implements `execute`, `executeSql`, and `executeVoid` to provide
        /// parity with the same member functions in `Repo`. Use `execute` to execute a generated query,
        /// use `executeSql` and `executeVoid` to execute SQL and a values tuple (e.g. when using
        /// handwritten SQL statements).
        pub fn connect(self: *AdaptedRepo) !Connection {
            return try self.adapter.connect(self);
        }

        /// Used internally to create a managed connection which is released on `repo.deinit()`.
        pub fn connectManaged(self: *AdaptedRepo) !Connection {
            if (self.connection == null) {
                self.connection = try self.connect();
            }
            return self.connection.?;
        }

        /// Release the repo's connection to the pool. If no connection is currently acquired then this
        /// is a no-op.
        pub fn release(self: *AdaptedRepo) void {
            if (self.connection) |connection| {
                self.connection = null;
                self.adapter.release(connection);
            }
        }

        pub fn executeInternal(
            self: *AdaptedRepo,
            query: anytype,
            caller_info: ?jetquery.debug.CallerInfo,
        ) !switch (@TypeOf(query).ResultContext) {
            .one => ?@TypeOf(query).ResultType,
            .many => jetquery.Result,
            .none => void,
        } {
            const connection = try self.connectManaged();
            errdefer self.release();

            try query.validateValues();
            try query.validateDelete();

            return try connection.execute(query, caller_info);
        }

        pub fn begin(self: *AdaptedRepo) !void {
            const connection = try self.connectManaged();
            try connection.executeVoid(
                "BEGIN",
                .{},
                try jetquery.debug.getCallerInfo(@returnAddress()),
            );
        }

        pub fn commit(self: *AdaptedRepo) !void {
            const connection = try self.connectManaged();
            try connection.executeVoid(
                "COMMIT",
                .{},
                try jetquery.debug.getCallerInfo(@returnAddress()),
            );
        }

        pub fn rollback(self: *AdaptedRepo) !void {
            const connection = try self.connectManaged();
            try connection.executeVoid(
                "ROLLBACK",
                .{},
                try jetquery.debug.getCallerInfo(@returnAddress()),
            );
        }

        /// Execute a query and return all of its results. Call `repo.free(result)` to free allocated
        /// memory.
        pub fn all(self: *AdaptedRepo, query: anytype) ![]@TypeOf(query).ResultType {
            var result = try self.executeInternal(
                query,
                try jetquery.debug.getCallerInfo(@returnAddress()),
            );
            return try result.all(query);
        }

        pub fn save(self: *AdaptedRepo, value: anytype) !void {
            // XXX: We have to include all (selected) values in the UPDATE because we can't generate a
            // type for the `update` params (which becomes a tuple passed to pg.zig) at runtime - ideally
            // we would only include modified values but we would need to generate all possible
            // combinations of updates in order to do this which is not practical. I don't know if there
            // is a way around this.
            // TODO: We can use pg.zig's dynamic statement binding to solve this.
            if (!self.isModified(value)) return;

            const primary_key = value.__jetquery_model.primary_key;

            comptime var size: usize = 0;
            comptime {
                for (std.meta.fields(@TypeOf(value))) |field| {
                    const is_primary_key = std.mem.eql(u8, field.name, primary_key);
                    if (!std.mem.startsWith(u8, field.name, "__") and !is_primary_key) {
                        size += 1;
                    }
                }
            }
            comptime var fields: [size]std.builtin.Type.StructField = undefined;
            comptime {
                var index: usize = 0;
                for (std.meta.fields(@TypeOf(value))) |field| {
                    if (!std.mem.startsWith(u8, field.name, "__") and !std.mem.eql(u8, field.name, "id")) {
                        fields[index] = field;
                        index += 1;
                    }
                }
            }

            const Update = @Type(.{
                .@"struct" = .{
                    .layout = .auto,
                    .fields = &fields,
                    .decls = &.{},
                    .is_tuple = false,
                },
            });

            var update: Update = undefined;

            inline for (std.meta.fields(Update)) |field| {
                @field(update, field.name) = @field(value, field.name);
            }

            const query = jetquery.Query(value.__jetquery_schema, value.__jetquery_model)
                .update(update)
                .where(.{ .id = value.id });

            try self.executeInternal(query, try jetquery.debug.getCallerInfo(@returnAddress()));
        }

        /// Insert a model instance into the database. Use `Schema.Model.init` to create a new record.
        /// ```zig
        /// const cat = Schema.Cat.init(.{ .name = "Hercules", .paws  = 4 });
        /// try repo.insert(cat);
        /// ```
        pub fn insert(self: *AdaptedRepo, value: anytype) !void {
            const query = jetquery.Query(
                value.__jetquery_schema,
                value.__jetquery_model,
            ).insert(value.__jetquery.args);

            try self.executeInternal(query, try jetquery.debug.getCallerInfo(@returnAddress()));
        }

        /// Delete a fetched record from the database. Record must have a primary key.
        /// ```zig
        /// const maybe_cat = try Query(Schema, .Cat).findBy(.{ .name = "Heracles" }).execute(repo);
        /// if (maybe_cat) |cat| try repo.delete(cat);
        /// ```
        pub fn delete(self: *AdaptedRepo, value: anytype) !void {
            const primary_key_field_name = value.__jetquery_model.primary_key;
            const Args = jetquery.fields.structType(
                &.{jetquery.fields.structField(
                    primary_key_field_name,
                    jetquery.fields.fieldType(@TypeOf(value), primary_key_field_name),
                )},
            );
            var args: Args = undefined;
            @field(args, primary_key_field_name) = @field(value, primary_key_field_name);

            const query = jetquery.Query(
                value.__jetquery_schema,
                value.__jetquery_model,
            ).delete().where(args);
            try self.executeInternal(query, try jetquery.debug.getCallerInfo(@returnAddress()));
        }

        pub const FieldState = struct {
            name: []const u8,
            modified: bool,
        };

        pub fn isModified(self: AdaptedRepo, value: anytype) bool {
            const field_states = self.fieldStates(value);
            for (field_states) |field_state| {
                if (field_state.modified) return true;
            }
            return false;
        }

        pub fn fieldStates(self: AdaptedRepo, value: anytype) []const FieldState {
            _ = self;
            comptime var size: usize = 0;
            inline for (std.meta.fields(@TypeOf(value))) |field| {
                if (comptime std.mem.startsWith(u8, field.name, "__")) size += 1;
            }
            var field_state: [size]FieldState = undefined;
            var index: usize = 0;

            const originals = value.__jetquery.original_values;
            inline for (std.meta.fields(@TypeOf(originals))) |field| {
                const modified = switch (@typeInfo(field.type)) {
                    .pointer => |info| std.mem.eql(
                        info.child,
                        @field(originals, field.name),
                        @field(value, field.name),
                    ),
                    else => @field(originals, field.name) == @field(value, field.name),
                };

                field_state[index] = .{ .name = field.name, .modified = modified };
                index += 1;
            }
            return &field_state;
        }

        /// Free a result's allocated memory. Supports flexible inputs and can be used in conjunction
        /// with `all()` and `findBy()` etc.:
        /// ```zig
        ///
        pub fn free(self: *AdaptedRepo, value: anytype) void {
            switch (@typeInfo(@TypeOf(value))) {
                .pointer => |info| switch (info.size) {
                    .Slice => {
                        for (value) |item| self.free(item);
                        self.allocator.free(value);
                    },
                    else => {},
                },
                .optional => {
                    if (value) |capture| return self.free(capture) else return;
                },
                else => {},
            }

            if (!comptime @hasField(@TypeOf(value), "__jetquery")) return;

            // Value may be modified after fetching so we only free the original value. If user messes
            // with internals and modifies original values then they will run into trouble.
            inline for (std.meta.fields(@TypeOf(value.__jetquery.original_values))) |field| {
                switch (@typeInfo(field.type)) {
                    .pointer => |info| {
                        switch (info.child) {
                            // TODO: Couple this with `maybeDupe` logic to make sure we stay consistent
                            // in which types need to be freed.
                            u8 => {
                                self.allocator.free(@field(value.__jetquery.original_values, field.name));
                            },
                            else => {},
                        }
                    },
                    .@"struct" => self.free(@field(value, field.name)),
                    else => {},
                }
            }

            inline for (value.__jetquery_relation_names) |relation_name| {
                switch (@typeInfo(@TypeOf(@field(value, relation_name)))) {
                    // belongs_to
                    .@"struct" => self.free(@field(value, relation_name)),
                    // has_many
                    .pointer => {
                        for (@field(value, relation_name)) |item| self.free(item);
                        self.allocator.free(@field(value, relation_name));
                    },
                    else => {},
                }
            }
        }

        pub fn generateId(self: *AdaptedRepo) i128 {
            // We can probably risk wrapping here if an app loads >i128 records without freeing them
            return self.result_id.fetchAdd(1, .monotonic);
        }

        /// Create a database table named `nme`. Pass `.{ .if_not_exists = true }` to use
        /// `CREATE TABLE IF NOT EXISTS` syntax.
        pub fn createTable(
            self: *AdaptedRepo,
            comptime name: []const u8,
            comptime columns: []const jetquery.schema.Column,
            comptime options: CreateTableOptions,
        ) !void {
            var buf = std.ArrayList(u8).init(self.allocator);
            defer buf.deinit();

            const writer = buf.writer();

            try writer.print(
                \\CREATE TABLE{s} {s} (
            , .{ if (options.if_not_exists) " IF NOT EXISTS" else "", self.adapter.identifier(name) });

            inline for (columns, 0..) |column, index| {
                if (column.timestamps) |timestamps| {
                    try timestamps.toSql(writer, self.adapter);
                    if (index < columns.len - 1) try writer.print(", ", .{});
                } else {
                    try writer.print(
                        \\{s}{s}{s}{s}{s}{s}{s}
                    , .{
                        self.adapter.identifier(column.name),
                        if (column.primary_key)
                            ""
                        else
                            self.adapter.columnTypeSql(column),
                        if (!column.primary_key and column.options.not_null)
                            self.adapter.notNullSql()
                        else
                            "",
                        if (column.primary_key) self.adapter.primaryKeySql(column) else "",
                        if (column.options.unique) self.adapter.uniqueColumnSql() else "",
                        if (column.options.reference) |reference|
                            self.adapter.referenceSql(reference)
                        else
                            "",
                        if (index < columns.len - 1) ", " else "",
                    });
                }
            }

            try writer.print(")", .{});
            const connection = try self.connectManaged();
            try connection.executeVoid(
                buf.items,
                &.{},
                try jetquery.debug.getCallerInfo(@returnAddress()),
            );

            inline for (columns) |column| {
                if (comptime !column.options.index) continue;
                try self.createIndex(name, &.{column.name}, .{ .name = column.options.index_name });
            }

            inline for (columns) |column| {
                if (comptime column.timestamps) |timestamps| {
                    if (timestamps.created_at) {
                        try self.createIndex(name, &.{jetquery.default_column_names.created_at}, .{});
                    }
                    if (timestamps.updated_at) {
                        try self.createIndex(name, &.{jetquery.default_column_names.updated_at}, .{});
                    }
                }
            }
        }

        /// Drop a database table named `name`. Pass `.{ .if_exists = true }` to use
        /// `DROP TABLE IF EXISTS` syntax.
        pub fn dropTable(self: *AdaptedRepo, comptime name: []const u8, options: DropTableOptions) !void {
            var buf = std.ArrayList(u8).init(self.allocator);
            defer buf.deinit();

            const writer = buf.writer();

            try writer.print(
                \\DROP TABLE{s} {s}
            , .{ if (options.if_exists) " IF EXISTS" else "", self.adapter.identifier(name) });

            const connection = try self.connectManaged();
            try connection.executeVoid(
                buf.items,
                &.{},
                try jetquery.debug.getCallerInfo(@returnAddress()),
            );
        }

        /// Create a new database in the current repo. Repo must be initialized with the appropriate user
        /// credentials for creating new databases.
        pub fn createDatabase(self: *AdaptedRepo, comptime name: []const u8, options: struct {}) !void {
            _ = options;
            var buf = std.ArrayList(u8).init(self.allocator);
            defer buf.deinit();

            const writer = buf.writer();

            try writer.print(
                \\CREATE DATABASE {s}
            , .{self.adapter.identifier(name)});

            const connection = try self.connectManaged();
            try connection.executeVoid(
                buf.items,
                &.{},
                try jetquery.debug.getCallerInfo(@returnAddress()),
            );
        }

        /// Create a new database in the current repo. Repo must be initialized with the appropriate user
        /// credentials for creating new databases.
        pub fn dropDatabase(self: *AdaptedRepo, comptime name: []const u8, options: DropDatabaseOptions) !void {
            var buf = std.ArrayList(u8).init(self.allocator);
            defer buf.deinit();

            const writer = buf.writer();

            try writer.print(
                \\DROP DATABASE{s} {s}
            , .{ if (options.if_exists) " IF EXISTS" else "", self.adapter.identifier(name) });

            const connection = try self.connectManaged();
            try connection.executeVoid(
                buf.items,
                &.{},
                try jetquery.debug.getCallerInfo(@returnAddress()),
            );
        }

        pub const CreateIndexOptions = struct {
            unique: bool = false,
            name: ?[]const u8 = null,
        };

        /// Create an index on the specified table name and column names. Optionally pass]
        /// `.{ .unique = true }` to create a unique constraint on the index.
        pub fn createIndex(
            self: *AdaptedRepo,
            comptime table_name: []const u8,
            comptime column_names: []const []const u8,
            comptime options: CreateIndexOptions,
        ) !void {
            const adapter = jetquery.adapters.Type(jetquery.adapter);
            const index_name = comptime options.name orelse adapter.indexName(
                table_name,
                column_names,
            );
            const sql = comptime adapter.createIndexSql(index_name, table_name, column_names, options);
            const connection = try self.connectManaged();
            try connection.executeVoid(
                sql,
                .{},
                try jetquery.debug.getCallerInfo(@returnAddress()),
            );
        }
    };
}

test "Repo" {
    try resetDatabase();

    var repo = try Repo(.postgresql).init(
        std.testing.allocator,
        .{
            .adapter = .{
                .database = "repo_test",
                .username = "postgres",
                .hostname = "127.0.0.1",
                .password = "password",
                .port = 5432,
            },
        },
    );
    defer repo.deinit();

    const Schema = struct {
        pub const Cat = jetquery.Table(
            @This(),
            "cats",
            struct { name: []const u8, paws: i32 },
            .{},
        );
    };

    try repo.createTable(
        "cats",
        &.{
            jetquery.schema.table.column("name", .string, .{}),
            jetquery.schema.table.column("paws", .integer, .{}),
        },
        .{ .if_not_exists = true },
    );

    try jetquery.Query(Schema, .Cat).insert(.{ .name = "Hercules", .paws = 4 }).execute(&repo);
    try jetquery.Query(Schema, .Cat).insert(.{ .name = "Princes", .paws = 4 }).execute(&repo);

    const coalesced_paws = jetquery.sql.column(i32, "coalesce(cats.paws, 3)").as(.coalesced_paws);

    const query = jetquery.Query(Schema, .Cat)
        .select(.{ .name, .paws, coalesced_paws })
        .where(.{ .paws = 4 });

    const cats = try repo.all(query);
    defer repo.free(cats);

    for (cats) |cat| {
        try std.testing.expectEqualStrings("Hercules", cat.name);
        try std.testing.expectEqual(4, cat.paws);
        try std.testing.expectEqual(4, cat.coalesced_paws);
        break;
    } else {
        try std.testing.expect(false);
    }

    const count_all = try query.count().execute(&repo);
    try std.testing.expectEqual(2, count_all);

    const count_distinct = try jetquery.Query(Schema, .Cat)
        .distinct(.{.paws})
        .count()
        .execute(&repo);
    try std.testing.expectEqual(1, count_distinct);
}

test "Repo.loadConfig" {
    try resetDatabase();

    // Loads default config file: `jetquery.config.zig`
    var repo = try Repo(.postgresql).loadConfig(std.testing.allocator, .{});
    defer repo.deinit();
    try std.testing.expect(repo.adapter == .postgresql);
}

test "relations" {
    try resetDatabase();

    var repo = try Repo(.postgresql).init(
        std.testing.allocator,
        .{
            .adapter = .{
                .database = "repo_test",
                .username = "postgres",
                .hostname = "127.0.0.1",
                .password = "password",
                .port = 5432,
            },
        },
    );
    defer repo.deinit();

    const Schema = struct {
        pub const Cat = jetquery.Table(
            @This(),
            "cats",
            struct { id: i32, human_id: ?i32, name: []const u8, paws: i32 },
            .{ .relations = .{ .human = jetquery.relation.belongsTo(.Human, .{}) } },
        );

        pub const Human = jetquery.Table(
            @This(),
            "humans",
            struct { id: i32, name: []const u8 },
            .{ .relations = .{ .cats = jetquery.relation.hasMany(.Cat, .{}) } },
        );
    };

    try repo.createTable(
        "cats",
        &.{
            jetquery.schema.table.column("id", .integer, .{}),
            jetquery.schema.table.column("human_id", .integer, .{}),
            jetquery.schema.table.column("name", .string, .{}),
            jetquery.schema.table.column("paws", .integer, .{}),
        },
        .{ .if_not_exists = true },
    );

    try repo.createTable(
        "humans",
        &.{
            jetquery.schema.table.column("id", .integer, .{}),
            jetquery.schema.table.column("name", .string, .{}),
        },
        .{ .if_not_exists = true },
    );

    try jetquery.Query(Schema, .Cat)
        .insert(.{ .id = 1, .name = "Hercules", .paws = 4, .human_id = 1 })
        .execute(&repo);
    try jetquery.Query(Schema, .Human)
        .insert(.{ .id = 1, .name = "Bob" })
        .execute(&repo);

    const query = jetquery.Query(Schema, .Cat)
        .include(.human, .{})
        .findBy(.{ .name = "Hercules" });

    const cat = try query.execute(&repo) orelse return try std.testing.expect(false);
    defer repo.free(cat);

    try std.testing.expectEqualStrings("Hercules", cat.name);
    try std.testing.expectEqual(4, cat.paws);
    try std.testing.expectEqualStrings("Bob", cat.human.name);

    const bob = try jetquery.Query(Schema, .Human)
        .include(.cats, .{})
        .findBy(.{ .name = "Bob" })
        .execute(&repo) orelse return try std.testing.expect(false);
    defer repo.free(bob);

    try std.testing.expectEqualStrings("Hercules", bob.cats[0].name);

    try repo.insert(Schema.Cat.init(.{
        .id = 2,
        .name = "Princes",
        .paws = std.crypto.random.int(u3),
        .human_id = bob.id,
    }));
    try repo.insert(Schema.Cat.init(.{
        .id = 3,
        .name = "Heracles",
        .paws = std.crypto.random.int(u3),
        .human_id = 1000,
    }));

    const bob_with_more_cats = try jetquery.Query(Schema, .Human)
        .include(.cats, .{})
        .findBy(.{ .name = "Bob" })
        .execute(&repo) orelse return try std.testing.expect(false);
    defer repo.free(bob_with_more_cats);

    try std.testing.expect(bob_with_more_cats.cats.len == 2);
    try std.testing.expectEqualStrings("Hercules", bob_with_more_cats.cats[0].name);
    try std.testing.expectEqualStrings("Princes", bob_with_more_cats.cats[1].name);

    try repo.insert(Schema.Human.init(.{
        .id = 2,
        .name = "Jane",
    }));

    const jane = try jetquery.Query(Schema, .Human)
        .include(.cats, .{})
        .findBy(.{ .name = "Jane" })
        .execute(&repo) orelse return try std.testing.expect(false);
    defer repo.free(jane);

    try std.testing.expect(jane.cats.len == 0);

    try repo.insert(Schema.Cat.init(.{
        .id = 4,
        .human_id = jane.id,
        .name = "Cindy",
        .paws = std.crypto.random.int(u3),
    }));

    try repo.insert(Schema.Cat.init(.{
        .id = 5,
        .human_id = jane.id,
        .name = "Garfield",
        .paws = std.crypto.random.int(u3),
    }));

    try repo.insert(Schema.Cat.init(.{
        .id = 6,
        .human_id = jane.id,
        .name = "Felix",
        .paws = std.crypto.random.int(u3),
    }));

    const humans = try jetquery.Query(Schema, .Human).include(.cats, .{}).orderBy(.name).all(&repo);
    defer repo.free(humans);

    try std.testing.expect(humans.len == 2);
    try std.testing.expect(humans[0].cats.len == 2);
    try std.testing.expect(humans[1].cats.len == 3);
    try std.testing.expectEqualStrings("Bob", humans[0].name);
    try std.testing.expectEqualStrings("Jane", humans[1].name);
    try std.testing.expectEqualStrings("Hercules", humans[0].cats[0].name);
    try std.testing.expectEqualStrings("Princes", humans[0].cats[1].name);
    try std.testing.expectEqualStrings("Jane", humans[1].name);
    try std.testing.expectEqualStrings("Cindy", humans[1].cats[0].name);
    try std.testing.expectEqualStrings("Garfield", humans[1].cats[1].name);
    try std.testing.expectEqualStrings("Felix", humans[1].cats[2].name);

    const jane_two_cats = try jetquery.Query(Schema, .Human).findBy(.{ .name = "Jane" })
        .include(.cats, .{ .limit = 2, .order_by = .{.name} })
        .execute(&repo);
    defer repo.free(jane_two_cats);
    try std.testing.expect(jane_two_cats.?.cats.len == 2);
    try std.testing.expectEqualStrings("Cindy", jane_two_cats.?.cats[0].name);
    try std.testing.expectEqualStrings("Felix", jane_two_cats.?.cats[1].name);
    // TODO: Merge rows on `next()`
    // const iterating_query = jetquery.Query(Schema, .Human).include(.cats, .{});
    // var humans_iterated = try iterating_query.execute(&repo);
    // defer humans_iterated.deinit();
    // while (try humans_iterated.next(iterating_query)) |human| {
    //     defer repo.free(human);
    //     std.debug.print("{s}\n", .{human.name});
    // }
}

test "timestamps" {
    try resetDatabase();

    var repo = try Repo(.postgresql).init(
        std.testing.allocator,
        .{
            .adapter = .{
                .database = "repo_test",
                .username = "postgres",
                .hostname = "127.0.0.1",
                .password = "password",
                .port = 5432,
            },
        },
    );
    defer repo.deinit();

    const Schema = struct {
        pub const Cat = jetquery.Table(
            @This(),
            "cats",
            struct {
                id: i32,
                name: []const u8,
                paws: i32,
                created_at: jetquery.DateTime,
                updated_at: jetquery.DateTime,
            },
            .{},
        );
    };

    try repo.createTable(
        "cats",
        &.{
            jetquery.schema.table.primaryKey("id", .{}),
            jetquery.schema.table.column("name", .string, .{}),
            jetquery.schema.table.column("paws", .integer, .{}),
            jetquery.schema.table.column("created_at", .datetime, .{}),
            jetquery.schema.table.column("updated_at", .datetime, .{}),
        },
        .{},
    );

    const now = std.time.microTimestamp();
    std.time.sleep(std.time.ns_per_ms);

    try jetquery.Query(Schema, .Cat).insert(.{ .name = "Hercules", .paws = 4 }).execute(&repo);

    const maybe_cat = try jetquery.Query(Schema, .Cat)
        .findBy(.{ .name = "Hercules" })
        .execute(&repo);

    if (maybe_cat) |cat| {
        defer repo.free(cat);
        try std.testing.expect(cat.created_at.microseconds() > now);
    }
}

test "save" {
    try resetDatabase();

    var repo = try Repo(.postgresql).init(
        std.testing.allocator,
        .{
            .adapter = .{
                .database = "repo_test",
                .username = "postgres",
                .hostname = "127.0.0.1",
                .password = "password",
                .port = 5432,
            },
        },
    );
    defer repo.deinit();

    const Schema = struct {
        pub const Cat = jetquery.Table(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32 },
            .{},
        );
    };

    try repo.createTable("cats", &.{
        jetquery.schema.table.column("id", .integer, .{}),
        jetquery.schema.table.column("name", .string, .{}),
        jetquery.schema.table.column("paws", .integer, .{}),
    }, .{ .if_not_exists = true });

    try jetquery.Query(Schema, .Cat)
        .insert(.{ .id = 1000, .name = "Hercules", .paws = 4 })
        .execute(&repo);

    var cat = try jetquery.Query(Schema, .Cat)
        .findBy(.{ .name = "Hercules" })
        .execute(&repo) orelse return std.testing.expect(false);
    defer repo.free(cat);

    cat.name = "Princes";
    try repo.save(cat);

    const updated_cat = try jetquery.Query(Schema, .Cat)
        .find(1000)
        .execute(&repo) orelse return std.testing.expect(false);
    defer repo.free(updated_cat);
    try std.testing.expectEqualStrings("Princes", updated_cat.name);
}

test "aggregate max()" {
    try resetDatabase();

    var repo = try Repo(.postgresql).init(
        std.testing.allocator,
        .{
            .adapter = .{
                .database = "repo_test",
                .username = "postgres",
                .hostname = "127.0.0.1",
                .password = "password",
                .port = 5432,
            },
        },
    );
    defer repo.deinit();

    try repo.createTable("cats", &.{
        jetquery.schema.table.column("name", .string, .{}),
        jetquery.schema.table.column("paws", .integer, .{}),
    }, .{ .if_not_exists = true });

    const Schema = struct {
        pub const Cat = jetquery.Table(
            @This(),
            "cats",
            struct { name: []const u8, paws: usize },
            .{},
        );
    };
    const sql = jetquery.sql;

    try repo.insert(Schema.Cat.init(.{ .name = "Hercules", .paws = 2 }));
    try repo.insert(Schema.Cat.init(.{ .name = "Hercules", .paws = 8 }));
    try repo.insert(Schema.Cat.init(.{ .name = "Hercules", .paws = 4 }));
    try repo.insert(Schema.Cat.init(.{ .name = "Princes", .paws = 100 }));
    try repo.insert(Schema.Cat.init(.{ .name = "Princes", .paws = 5 }));
    try repo.insert(Schema.Cat.init(.{ .name = "Princes", .paws = 2 }));

    const cats = try jetquery.Query(Schema, .Cat)
        .select(.{ .name, sql.max(.paws) })
        .groupBy(.{.name})
        .orderBy(.{.name})
        .all(&repo);
    defer repo.free(cats);

    try std.testing.expectEqualStrings(cats[0].name, "Hercules");
    try std.testing.expect(cats[0].max__paws == 8);
    try std.testing.expectEqualStrings(cats[1].name, "Princes");
    try std.testing.expect(cats[1].max__paws == 100);
}

test "aggregate count() with HAVING" {
    try resetDatabase();

    var repo = try Repo(.postgresql).init(
        std.testing.allocator,
        .{
            .adapter = .{
                .database = "repo_test",
                .username = "postgres",
                .hostname = "127.0.0.1",
                .password = "password",
                .port = 5432,
            },
        },
    );
    defer repo.deinit();

    try repo.createTable("cats", &.{
        jetquery.schema.table.column("name", .string, .{}),
        jetquery.schema.table.column("paws", .integer, .{}),
    }, .{ .if_not_exists = true });

    const Schema = struct {
        pub const Cat = jetquery.Table(
            @This(),
            "cats",
            struct { name: []const u8, paws: usize },
            .{},
        );
    };

    try repo.insert(Schema.Cat.init(.{ .name = "Hercules", .paws = 2 }));
    try repo.insert(Schema.Cat.init(.{ .name = "Hercules", .paws = 8 }));
    try repo.insert(Schema.Cat.init(.{ .name = "Hercules", .paws = 4 }));
    try repo.insert(Schema.Cat.init(.{ .name = "Princes", .paws = 100 }));
    try repo.insert(Schema.Cat.init(.{ .name = "Princes", .paws = 5 }));
    try repo.insert(Schema.Cat.init(.{ .name = "Princes", .paws = 2 }));

    const sql = jetquery.sql;

    const cats = try jetquery.Query(Schema, .Cat)
        .select(.{ .name, sql.max(.paws).as("maximum_paws") })
        .where(.{ .{ .name = "Hercules" }, .OR, .{ .name, .like, "Pri%" } })
        .groupBy(.{.name})
        .having(.{ .{ sql.count(.name), .gt_eql, "3" }, .OR, .{ sql.count(.name), .lt_eql, 3 } })
        .orderBy(.{.name})
        .all(&repo);
    defer repo.free(cats);

    try std.testing.expectEqualStrings(cats[0].name, "Hercules");
    try std.testing.expect(cats[0].maximum_paws == 8);
    try std.testing.expectEqualStrings(cats[1].name, "Princes");
    try std.testing.expect(cats[1].maximum_paws == 100);
}

test "missing config options" {
    try resetDatabase();

    const repo = Repo(.postgresql).init(std.testing.allocator, .{ .adapter = .{} });
    try std.testing.expectError(error.JetQueryConfigError, repo);
}

test "transactions" {
    try resetDatabase();

    var repo = try Repo(.postgresql).init(
        std.testing.allocator,
        .{
            .adapter = .{
                .database = "repo_test",
                .username = "postgres",
                .hostname = "127.0.0.1",
                .password = "password",
                .port = 5432,
            },
        },
    );
    defer repo.deinit();

    try repo.createTable("cats", &.{
        jetquery.schema.table.column("name", .string, .{}),
        jetquery.schema.table.column("paws", .integer, .{}),
    }, .{ .if_not_exists = true });

    const Schema = struct {
        pub const Cat = jetquery.Table(
            @This(),
            "cats",
            struct { name: []const u8, paws: i32 },
            .{},
        );
    };

    try repo.begin();
    try repo.insert(Schema.Cat.init(.{ .name = "Hercules", .paws = 4 }));
    try repo.rollback();

    const no_cat = try jetquery.Query(Schema, .Cat)
        .findBy(.{ .name = "Hercules" })
        .execute(&repo);
    defer repo.free(no_cat);
    try std.testing.expect(no_cat == null);

    try repo.begin();
    try repo.insert(Schema.Cat.init(.{ .name = "Hercules", .paws = 4 }));
    try repo.commit();

    const yes_cat = try jetquery.Query(Schema, .Cat)
        .findBy(.{ .name = "Hercules" })
        .execute(&repo);
    defer repo.free(yes_cat);
    try std.testing.expect(yes_cat != null);
}

fn resetDatabase() !void {
    var repo = try Repo(.postgresql).init(
        std.testing.allocator,
        .{
            .adapter = .{
                .database = "postgres",
                .username = "postgres",
                .hostname = "127.0.0.1",
                .password = "password",
                .port = 5432,
            },
        },
    );
    defer repo.deinit();
    try repo.dropDatabase("repo_test", .{ .if_exists = true });
    try repo.createDatabase("repo_test", .{});
}
