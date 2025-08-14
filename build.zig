const std = @import("std");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib = b.addLibrary(.{
        .name = "jetquery",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/jetquery.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    b.installArtifact(lib);

    const pg_dep = b.dependency("pg", .{ .target = target, .optimize = optimize });
    const jetcommon_dep = b.dependency("jetcommon", .{ .target = target, .optimize = optimize });
    const jetcommon_module = jetcommon_dep.module("jetcommon");

    lib.root_module.addImport("pg", pg_dep.module("pg"));
    lib.root_module.addImport("jetcommon", jetcommon_module);

    const config_path = b.option([]const u8, "jetquery_config_path", "JetQuery configuration file path") orelse "jetquery.config.zig";
    const config_module = if (try fileExist(config_path))
        b.createModule(.{ .root_source_file = .{ .cwd_relative = config_path } })
    else
        b.createModule(.{ .root_source_file = b.path("src/default_config.zig") });

    const jetquery_module = b.addModule("jetquery", .{ .root_source_file = b.path("src/jetquery.zig") });
    jetquery_module.addImport("pg", pg_dep.module("pg"));
    jetquery_module.addImport("jetcommon", jetcommon_module);
    jetquery_module.addImport("jetquery.config", config_module);

    const migrations_path = b.option([]const u8, "jetquery_migrations_path", "Migrations path") orelse
        "migrations";
    const seeders_path = b.option([]const u8, "jetquery_seeders_path", "Seeders path") orelse
        "seeders";
    const test_filters = b.option([]const []const u8, "test-filter", "Skip tests that do not match any filter") orelse &[0][]const u8{};
    const lib_unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/jetquery.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = test_filters,
    });
    lib_unit_tests.root_module.addImport("pg", pg_dep.module("pg"));
    lib_unit_tests.root_module.addImport("jetcommon", jetcommon_module);
    lib_unit_tests.root_module.addImport("jetquery.config", config_module);

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);

    const exe_generate_migrations = b.addExecutable(.{
        .name = "migrations",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/generate_migrations.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const migration_unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/jetquery/Migrate.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = test_filters,
    });
    const run_migration_unit_tests = b.addRunArtifact(migration_unit_tests);
    test_step.dependOn(&run_migration_unit_tests.step);
    migration_unit_tests.step.dependOn(&exe_generate_migrations.step);

    const run_generate_migrations_cmd = b.addRunArtifact(exe_generate_migrations);
    const generated_migrations_path = run_generate_migrations_cmd.addOutputFileArg("migrations.zig");

    for (try findFilesSorted(b.allocator, migrations_path)) |path| {
        run_generate_migrations_cmd.addFileArg(.{ .cwd_relative = path });
    }

    const migrations_module = b.addModule(
        "jetquery_migrations",
        .{ .root_source_file = generated_migrations_path },
    );
    migrations_module.addImport("jetquery", jetquery_module);
    migrations_module.addImport("jetquery.config", config_module);
    migration_unit_tests.root_module.addImport("migrations", migrations_module);
    migration_unit_tests.root_module.addImport("jetquery", jetquery_module);
    migration_unit_tests.root_module.addImport("jetcommon", jetcommon_module);

    const exe_generate_seeder = b.addExecutable(.{
        .name = "seed",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/generate_seeders.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const seed_unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/jetquery/Seed.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = test_filters,
    });
    const run_seed_unit_tests = b.addRunArtifact(seed_unit_tests);
    test_step.dependOn(&run_seed_unit_tests.step);
    seed_unit_tests.step.dependOn(&exe_generate_seeder.step);

    const run_generate_seeders_cmd = b.addRunArtifact(exe_generate_seeder);
    const generated_seeders_path = run_generate_seeders_cmd.addOutputFileArg("seeders.zig");

    for (try findFilesSorted(b.allocator, seeders_path)) |path| {
        run_generate_seeders_cmd.addFileArg(.{ .cwd_relative = path });
    }

    const jetquery_migrate_module = b.addModule(
        "jetquery_migrate",
        .{ .root_source_file = b.path("src/jetquery/Migrate.zig") },
    );
    jetquery_migrate_module.addImport("jetquery", jetquery_module);
    jetquery_migrate_module.addImport("migrations", migrations_module);
    jetquery_migrate_module.addImport("jetquery.config", config_module);
    jetquery_migrate_module.addImport("jetcommon", jetcommon_module);

    const seeders_module = b.addModule(
        "jetquery_seeders",
        .{ .root_source_file = generated_seeders_path },
    );
    seeders_module.addImport("jetquery", jetquery_module);
    seeders_module.addImport("jetquery.config", config_module);
    seed_unit_tests.root_module.addImport("migrations", migrations_module);
    seed_unit_tests.root_module.addImport("jetquery_migrate", jetquery_migrate_module);
    seed_unit_tests.root_module.addImport("seeders", seeders_module);
    seed_unit_tests.root_module.addImport("jetquery", jetquery_module);
    seed_unit_tests.root_module.addImport("jetcommon", jetcommon_module);

    const jetquery_seeder_module = b.addModule(
        "jetquery_seeder",
        .{ .root_source_file = b.path("src/jetquery/Seed.zig") },
    );
    jetquery_seeder_module.addImport("jetquery", jetquery_module);
    jetquery_seeder_module.addImport("seeders", seeders_module);
    jetquery_seeder_module.addImport("jetquery.config", config_module);
    jetquery_seeder_module.addImport("jetcommon", jetcommon_module);

    const jetquery_reflect_module = b.addModule(
        "jetquery_reflect",
        .{ .root_source_file = b.path("src/jetquery/reflection/Reflect.zig") },
    );
    jetquery_reflect_module.addImport("jetquery", jetquery_module);
    jetquery_reflect_module.addImport("migrations", migrations_module);
    jetquery_reflect_module.addImport("jetquery.config", config_module);
    jetquery_reflect_module.addImport("jetcommon", jetcommon_module);

    const reflect_unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/jetquery/reflection/Reflect.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = test_filters,
    });
    const run_reflect_unit_tests = b.addRunArtifact(reflect_unit_tests);
    reflect_unit_tests.root_module.addImport("jetquery", jetquery_module);
    reflect_unit_tests.root_module.addImport("jetcommon", jetcommon_module);
    test_step.dependOn(&run_reflect_unit_tests.step);
}

fn findFilesSorted(allocator: std.mem.Allocator, path: []const u8) ![][]const u8 {
    const absolute_path = if (std.fs.path.isAbsolute(path))
        path
    else
        std.fs.cwd().realpathAlloc(allocator, path) catch |err| {
            switch (err) {
                error.FileNotFound => return &.{},
                else => return err,
            }
        };

    var dir = std.fs.openDirAbsolute(absolute_path, .{ .iterate = true }) catch |err| {
        switch (err) {
            error.FileNotFound => return &.{},
            else => return err,
        }
    };
    defer dir.close();

    var files = std.ArrayList([]const u8).init(allocator);

    var it = dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .file) continue;
        try files.append(try std.fs.path.join(allocator, &.{ absolute_path, entry.name }));
    }

    std.mem.sort([]const u8, files.items, {}, cmpString);
    return try files.toOwnedSlice();
}

fn cmpString(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs).compare(.lt);
}

fn fileExist(path: []const u8) !bool {
    const file = std.fs.cwd().openFile(path, .{}) catch |err| {
        switch (err) {
            error.FileNotFound => return false,
            else => return err,
        }
    };

    file.close();

    return true;
}
