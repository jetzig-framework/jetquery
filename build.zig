const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib = b.addStaticLibrary(.{
        .name = "jetquery",
        .root_source_file = b.path("src/jetquery.zig"),
        .target = target,
        .optimize = optimize,
    });

    b.installArtifact(lib);

    const pg_module = b.dependency("pg", .{ .target = target, .optimize = optimize });
    lib.root_module.addImport("pg", pg_module.module("pg"));
    _ = b.addModule("jetquery", .{ .root_source_file = b.path("src/jetquery.zig") });

    const migrations_path = b.option([]const u8, "jetquery_migrations_path", "Migrations path") orelse "migrations";
    const lib_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/jetquery.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib_unit_tests.root_module.addImport("pg", pg_module.module("pg"));

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);

    const exe_generate_migrations = b.addExecutable(.{
        .name = "migrations",
        .root_source_file = b.path("src/generate_migrations.zig"),
        .target = target,
        .optimize = optimize,
    });

    const run_generate_migrations_cmd = b.addRunArtifact(exe_generate_migrations);
    const generated_migrations_path = run_generate_migrations_cmd.addOutputFileArg("migrations.zig");
    run_generate_migrations_cmd.addArg(migrations_path);
    const migrations_module = b.createModule(.{ .root_source_file = generated_migrations_path });
    lib.root_module.addImport("migrations", migrations_module);
}
