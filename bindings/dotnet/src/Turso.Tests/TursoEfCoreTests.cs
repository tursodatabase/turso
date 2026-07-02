using AwesomeAssertions;
using Microsoft.EntityFrameworkCore;
using System.Text.RegularExpressions;
using Turso.Data.Sqlite;

namespace Turso.Tests;

public class TursoEfCoreTests
{
    [Test]
    public async Task UseTursoCreatesTursoConnectionAndCreatesSchema()
    {
        using var database = TemporaryDatabase.Create();
        await using var context = CreateContext(database.ConnectionString);

        await context.Database.EnsureCreatedAsync();

        context.Database.GetDbConnection().Should().BeOfType<SqliteConnection>();
        (await context.Database.CanConnectAsync()).Should().BeTrue();
    }

    [Test]
    public async Task DynamicLinqCompositionMaterializesAsync()
    {
        using var database = TemporaryDatabase.Create();
        await using var context = CreateContext(database.ConnectionString);
        await SeedAsync(context);

        const string search = "example";
        IQueryable<Customer> query = context.Customers;
        query = query.Where(customer => customer.Name.Contains("a"));
        query = query.Where(customer => customer.Email.Contains(search));

        var customers = await query
            .OrderBy(customer => customer.Name)
            .Skip(0)
            .Take(2)
            .ToListAsync();

        customers.Select(customer => customer.Name).Should().Equal("Ada", "Clara");
    }

    [Test]
    public void SqlGenerationCoversSqliteLinqFeaturesWithoutOpeningDatabase()
    {
        using var database = TemporaryDatabase.Create();
        using var context = CreateContext(database.ConnectionString);

        context.Customers
            .Where(customer => EF.Functions.Like(customer.Email, "%@example.com")
                               && Regex.IsMatch(customer.Name, "(?=A)A"))
            .ToQueryString()
            .Should()
            .Contain("LIKE")
            .And.Contain("REGEXP");

        context.Orders
            .Where(order => order.Total + 1m > 10m)
            .OrderBy(order => order.Total)
            .ToQueryString()
            .Should()
            .Contain("ef_add")
            .And.Contain("ef_compare")
            .And.Contain("COLLATE EF_DECIMAL");

        context.Customers
            .Include(customer => customer.Orders.Where(order => order.Total > 0m))
            .AsSplitQuery()
            .ToQueryString()
            .Should()
            .Contain("SELECT");
    }

    [Test]
    public async Task NavigationPropertiesTranslateToJoins()
    {
        using var database = TemporaryDatabase.Create();
        await using var context = CreateContext(database.ConnectionString);
        await SeedAsync(context);

        var rows = await context.Customers
            .Where(customer => customer.Orders.Any(order => order.Total > 10m))
            .OrderBy(customer => customer.Name)
            .Select(customer => new
            {
                customer.Name,
                OrderCount = customer.Orders.Count,
                Total = customer.Orders.Sum(order => order.Total)
            })
            .ToListAsync();

        rows.Select(row => (row.Name, row.OrderCount, row.Total))
            .Should()
            .Equal(("Clara", 1, 20.00m));
    }

    [Test]
    public async Task IncludeAndSplitQueryMaterializeNavigationCollections()
    {
        using var database = TemporaryDatabase.Create();
        await using var context = CreateContext(database.ConnectionString);
        await SeedAsync(context);

        var customers = await context.Customers
            .Include(customer => customer.Orders.Where(order => order.Total > 0m))
            .AsSplitQuery()
            .OrderBy(customer => customer.Name)
            .ToListAsync();

        customers.Select(customer => (customer.Name, customer.Orders.Count))
            .Should()
            .Equal(("Ada", 2), ("Bob", 1), ("Clara", 1));
    }

    [Test]
    public async Task RawSqlInterpolatedComposesWithLinq()
    {
        using var database = TemporaryDatabase.Create();
        await using var context = CreateContext(database.ConnectionString);
        await SeedAsync(context);

        var customers = await context.Customers
            .FromSqlInterpolated($"SELECT * FROM Customers WHERE IsActive = {true}")
            .Where(customer => customer.Email.Contains("example"))
            .OrderBy(customer => customer.Name)
            .ToListAsync();

        customers.Select(customer => customer.Name).Should().Equal("Ada", "Clara");
    }

    [Test]
    public async Task AggregatesGroupingOrderingAndPagingUseEfSqliteHelpers()
    {
        using var database = TemporaryDatabase.Create();
        await using var context = CreateContext(database.ConnectionString);
        await SeedAsync(context);

        var totals = await context.Orders
            .GroupBy(order => order.CustomerId)
            .Select(group => new
            {
                CustomerId = group.Key,
                Count = group.Count(),
                Sum = group.Sum(order => order.Total),
                Average = group.Average(order => order.Total)
            })
            .OrderByDescending(group => group.Sum)
            .Skip(0)
            .Take(2)
            .ToListAsync();

        totals.Select(total => (total.Count, total.Sum, total.Average))
            .Should()
            .Equal((1, 20.00m, 20.00m), (2, 12.75m, 6.375m));
    }

    [Test]
    public async Task DecimalOrderingUsesNumericCollation()
    {
        using var database = TemporaryDatabase.Create();
        await using var context = CreateContext(database.ConnectionString);

        context.Orders
            .OrderBy(order => order.Total)
            .ToQueryString()
            .Should()
            .Contain("COLLATE EF_DECIMAL");

        await context.Database.EnsureCreatedAsync();

        var customer = new Customer { Name = "Frank", Email = "frank@example.com", IsActive = true };
        context.Orders.AddRange(
            new Order { Customer = customer, Total = 10m, CreatedAt = new DateTime(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc) },
            new Order { Customer = customer, Total = 9m, CreatedAt = new DateTime(2026, 1, 2, 10, 0, 0, DateTimeKind.Utc) });
        await context.SaveChangesAsync();

        var totals = await context.Orders
            .OrderBy(order => order.Total)
            .Select(order => order.Total)
            .ToListAsync();

        totals.Should().Equal(9m, 10m);
    }

    [Test]
    public async Task SaveChangesAsyncPreservesHighPrecisionDecimal()
    {
        using var database = TemporaryDatabase.Create();
        await using var context = CreateContext(database.ConnectionString);
        await context.Database.EnsureCreatedAsync();
        const decimal expected = 1234567890123456789012345678m;

        var customer = new Customer { Name = "Grace", Email = "grace@example.com", IsActive = true };
        context.Orders.Add(new Order
        {
            Customer = customer,
            Total = expected,
            CreatedAt = new DateTime(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc)
        });
        await context.SaveChangesAsync();

        var total = await context.Orders.Select(order => order.Total).SingleAsync();

        total.Should().Be(expected);
    }

    [Test]
    public async Task ExecuteUpdateDeleteAndTransactionRollbackWork()
    {
        using var database = TemporaryDatabase.Create();
        await using var context = CreateContext(database.ConnectionString);
        await SeedAsync(context);

        var updated = await context.Customers
            .Where(customer => customer.IsActive)
            .ExecuteUpdateAsync(setters => setters.SetProperty(customer => customer.IsActive, false));
        updated.Should().Be(2);

        await using var transaction = await context.Database.BeginTransactionAsync();
        context.Customers.Add(new Customer { Name = "Helen", Email = "helen@example.com", IsActive = true });
        await context.SaveChangesAsync();
        await transaction.RollbackAsync();

        (await context.Customers.CountAsync(customer => customer.Name == "Helen")).Should().Be(0);

        var deleted = await context.Customers
            .Where(customer => !customer.IsActive)
            .ExecuteDeleteAsync();
        deleted.Should().Be(3);
    }

    [Test]
    public async Task SaveChangesAsyncInsertsUpdatesAndDeletes()
    {
        using var database = TemporaryDatabase.Create();
        await using var context = CreateContext(database.ConnectionString);
        await context.Database.EnsureCreatedAsync();

        var customer = new Customer { Name = "Dora", Email = "dora@example.com", IsActive = true };
        context.Customers.Add(customer);
        await context.SaveChangesAsync();

        customer.Id.Should().BeGreaterThan(0);

        customer.Email = "dora.updated@example.com";
        await context.SaveChangesAsync();

        (await context.Customers.SingleAsync()).Email.Should().Be("dora.updated@example.com");

        context.Customers.Remove(customer);
        await context.SaveChangesAsync();

        (await context.Customers.CountAsync()).Should().Be(0);
    }

    [Test]
    public async Task ExistingTursoConnectionCanBeUsedWithEfCore()
    {
        using var database = TemporaryDatabase.Create();
        await using var connection = new SqliteConnection(database.ConnectionString);
        await using var context = CreateContext(connection);

        await context.Database.EnsureCreatedAsync();

        context.Customers.Add(new Customer { Name = "Eve", Email = "eve@example.com", IsActive = true });
        await context.SaveChangesAsync();

        (await context.Customers.SingleAsync()).Name.Should().Be("Eve");
    }

    [Test]
    public async Task EnsureDeletedRemovesWalSidecarFiles()
    {
        using var database = TemporaryDatabase.Create();
        await using var context = CreateContext(database.ConnectionString);
        await context.Database.EnsureCreatedAsync();
        await context.Database.CloseConnectionAsync();

        var walPath = database.Path + "-wal";
        var shmPath = database.Path + "-shm";
        await File.WriteAllTextAsync(walPath, "wal");
        await File.WriteAllTextAsync(shmPath, "shm");

        (await context.Database.EnsureDeletedAsync()).Should().BeTrue();

        File.Exists(database.Path).Should().BeFalse();
        File.Exists(walPath).Should().BeFalse();
        File.Exists(shmPath).Should().BeFalse();
    }

    private static TestDbContext CreateContext(string connectionString)
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseTurso(connectionString)
            .Options;

        return new TestDbContext(options);
    }

    private static TestDbContext CreateContext(SqliteConnection connection)
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseTurso(connection)
            .Options;

        return new TestDbContext(options);
    }

    private static async Task SeedAsync(TestDbContext context)
    {
        await context.Database.EnsureCreatedAsync();

        var ada = new Customer { Name = "Ada", Email = "ada@example.com", IsActive = true };
        var bob = new Customer { Name = "Bob", Email = "bob@internal.local", IsActive = false };
        var clara = new Customer { Name = "Clara", Email = "clara@example.com", IsActive = true };

        context.Customers.AddRange(ada, bob, clara);
        context.Orders.AddRange(
            new Order { Customer = ada, Total = 5.50m, CreatedAt = new DateTime(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc) },
            new Order { Customer = ada, Total = 7.25m, CreatedAt = new DateTime(2026, 1, 2, 10, 0, 0, DateTimeKind.Utc) },
            new Order { Customer = bob, Total = 3.00m, CreatedAt = new DateTime(2026, 1, 3, 10, 0, 0, DateTimeKind.Utc) },
            new Order { Customer = clara, Total = 20.00m, CreatedAt = new DateTime(2026, 1, 4, 10, 0, 0, DateTimeKind.Utc) });

        await context.SaveChangesAsync();
    }

    private sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
    {
        public DbSet<Customer> Customers => Set<Customer>();

        public DbSet<Order> Orders => Set<Order>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Customer>(entity =>
            {
                entity.HasKey(customer => customer.Id);
                entity.Property(customer => customer.Id).ValueGeneratedOnAdd();
                entity.Property(customer => customer.Name).IsRequired();
                entity.Property(customer => customer.Email).IsRequired();
                entity.HasMany(customer => customer.Orders)
                    .WithOne(order => order.Customer)
                    .HasForeignKey(order => order.CustomerId);
            });

            modelBuilder.Entity<Order>(entity =>
            {
                entity.HasKey(order => order.Id);
                entity.Property(order => order.Id).ValueGeneratedOnAdd();
                entity.Property(order => order.Total).HasColumnType("TEXT");
                entity.Property(order => order.CreatedAt).IsRequired();
            });
        }
    }

    private sealed class Customer
    {
        public long Id { get; set; }

        public string Name { get; set; } = "";

        public string Email { get; set; } = "";

        public bool IsActive { get; set; }

        public List<Order> Orders { get; set; } = [];
    }

    private sealed class Order
    {
        public long Id { get; set; }

        public long CustomerId { get; set; }

        public Customer Customer { get; set; } = null!;

        public decimal Total { get; set; }

        public DateTime CreatedAt { get; set; }
    }

    private sealed class TemporaryDatabase : IDisposable
    {
        private TemporaryDatabase(string path)
        {
            Path = path;
            ConnectionString = $"Data Source={path}";
        }

        public string Path { get; }

        public string ConnectionString { get; }

        public static TemporaryDatabase Create()
        {
            var path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), System.IO.Path.GetRandomFileName() + ".db");
            return new TemporaryDatabase(path);
        }

        public void Dispose()
        {
            if (File.Exists(Path))
                File.Delete(Path);
            if (File.Exists(Path + "-wal"))
                File.Delete(Path + "-wal");
            if (File.Exists(Path + "-shm"))
                File.Delete(Path + "-shm");
        }
    }
}
