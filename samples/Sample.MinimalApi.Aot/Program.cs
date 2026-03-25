using System.Text.Json.Serialization;
using InterBus;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonSerializerContext.Default);
});

builder.Services.AddInterBus(bus =>
{
    bus.UseSystemTextJsonSerializer(AppJsonSerializerContext.Default.Options);
    bus.AddInMemoryTransport();
    bus.AddConsumer<TodoNotifier>();
});

var app = builder.Build();

List<Todo> sampleTodos =
[
    new(1, "Walk the dog"),
    new(2, "Do the dishes", DateOnly.FromDateTime(DateTime.Now)),
    new(3, "Do the laundry", DateOnly.FromDateTime(DateTime.Now.AddDays(1))),
    new(4, "Clean the bathroom"),
    new(5, "Clean the car", DateOnly.FromDateTime(DateTime.Now.AddDays(2)))
];

var todosApi = app.MapGroup("/todos");
todosApi.MapGet("/", () => sampleTodos)
    .WithName("GetTodos");

todosApi.MapGet("/{id}", Results<Ok<Todo>, NotFound> (int id) =>
        sampleTodos.FirstOrDefault(a => a.Id == id) is { } todo
            ? TypedResults.Ok(todo)
            : TypedResults.NotFound())
    .WithName("GetTodoById");

todosApi.MapPost("/", async Task<Created<Todo>> ([FromServices] IMessagePublisher publisher, Todo todo) =>
    {
        sampleTodos.Add(todo);

        await publisher.Publish(new TaskAdded(todo.Id, todo.Title));

        return TypedResults.Created($"/todos/{todo.Id}", todo);
    })
    .WithName("CreateTodo");

todosApi.MapPut("/{id}/complete",
    async Task<Results<Ok, NotFound>> ([FromServices] IMessagePublisher publisher, int id) =>
    {
        var todo = sampleTodos.FirstOrDefault(x => x.Id == id);

        if (todo is null)
            return TypedResults.NotFound();

        var completedTodo = todo with
        {
            IsComplete = true
        };

        sampleTodos.Remove(todo);
        sampleTodos.Add(completedTodo);

        await publisher.Publish(new TaskCompleted(id));

        return TypedResults.Ok();
    });

app.Run();

public record Todo(int Id, string Title, DateOnly? DueBy = null, bool IsComplete = false);

public record TaskAdded(int Id, string Title);

public record TaskCompleted(int Id);

public class TodoNotifier(ILogger<TodoNotifier> logger) : IConsumer<TaskAdded>, IConsumer<TaskCompleted>
{
    public Task Consume(TaskAdded message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Task added: {Id}: {Title}", message.Id, message.Title);
        return Task.CompletedTask;
    }

    public Task Consume(TaskCompleted message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Task completed: {Id}", message.Id);
        return Task.CompletedTask;
    }
}

[JsonSerializable(typeof(List<Todo>))]
[JsonSerializable(typeof(TaskAdded))]
[JsonSerializable(typeof(TaskCompleted))]
internal partial class AppJsonSerializerContext : JsonSerializerContext
{
}
