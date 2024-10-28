const std = @import("std");

const jetquery = @import("../jetquery.zig");

/// Abstraction of a database table. Define a schema with:
/// ```zig
/// const Schema = struct {
///     pub const Cat = Table("cats", struct { name: []const u8, paws: usize }, .{});
/// };
/// ```
pub fn Model(Schema: type, comptime table_name: []const u8, T: type, options: anytype) type {
    return struct {
        // TODO: Implement `format()`

        pub const Definition = T;
        const Self = @This();
        pub const name = table_name;
        pub const info = .{ .schema = Schema };

        pub const relations = if (@hasField(
            @TypeOf(options),
            "relations",
        )) options.relations else .{};

        pub const primary_key = if (@hasField(
            @TypeOf(options),
            "primary_key",
        )) jetquery.util.stringMaybeEnum(options.primary_key) else "id";

        pub fn init(args: anytype) RecordType(@TypeOf(args)) {
            var record: RecordType(@TypeOf(args)) = undefined;

            record.__jetquery = .{ .args = args };

            inline for (std.meta.fields(@TypeOf(args))) |field| {
                @field(record, field.name) = @field(args, field.name);
            }
            return record;
        }

        pub fn Relation(comptime relation_name: []const u8) type {
            comptime {
                for (relations) |relation| {
                    if (std.mem.eql(u8, relation.relation_name, relation_name)) {
                        return relation;
                    }
                }
                @compileError(std.fmt.comptimePrint(
                    "Failed matching relation `{s}` on `{s}`",
                    .{ relation_name, name },
                ));
            }
        }

        pub fn columns() [std.meta.fields(Definition).len]jetquery.columns.Column {
            comptime {
                const fields = std.meta.fields(Definition);
                var buf: [fields.len]jetquery.columns.Column = undefined;
                for (fields, 0..) |field, index| {
                    buf[index] = .{
                        .name = field.name,
                        .table = @This(),
                        .type = field.type,
                    };
                }
                return buf;
            }
        }

        pub fn column(comptime column_name: []const u8) jetquery.columns.Column {
            comptime {
                return for (columns()) |col| {
                    if (std.mem.eql(u8, column_name, col.name)) break col;
                } else @compileError(std.fmt.comptimePrint(
                    "No column named `{s}` defined in Schema for `{s}'",
                    .{ column_name, table_name },
                ));
            }
        }

        fn RecordType(Args: type) type {
            comptime {
                const fields = std.meta.fields(T);
                var struct_fields: [fields.len + 3]std.builtin.Type.StructField = undefined;

                for (fields, 0..) |field, index| {
                    struct_fields[index] = jetquery.fields.structField(field.name, field.type);
                }

                struct_fields[fields.len] = jetquery.fields.structFieldComptime(
                    "__jetquery_model",
                    @This(),
                );

                struct_fields[fields.len + 1] = jetquery.fields.structFieldComptime(
                    "__jetquery_schema",
                    Schema,
                );

                const args_field = jetquery.fields.structField("args", Args);
                const JetQuery = jetquery.fields.structType(&.{args_field});

                struct_fields[fields.len + 2] = jetquery.fields.structField(
                    "__jetquery",
                    JetQuery,
                );

                return jetquery.fields.structType(&struct_fields);
            }
        }

        pub fn defaultForeignKey() []const u8 {
            comptime {
                for (@typeInfo(Schema).@"struct".decls) |decl| {
                    const table = @field(Schema, decl.name);

                    if (std.mem.eql(u8, table.name, @This().name)) {
                        var buf: [decl.name.len]u8 = undefined;
                        return std.ascii.lowerString(&buf, decl.name) ++ "_id";
                    }
                }

                @compileError("Failed matching `" ++ @typeName(@This()) ++ "` in schema.");
            }
        }

        pub fn defaultOrderBy() []const jetquery.sql.OrderClause {
            if (!@hasField(Definition, primary_key)) return &.{};

            return &.{.{ .column = column(primary_key), .direction = .ascending }};
        }
    };
}
