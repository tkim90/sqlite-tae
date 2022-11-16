#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

typedef struct {
  char* buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

typedef enum { META_COMMAND_SUCCESS, META_COMMAND_UNRECOGNIZED_COMMAND } MetaCommandResult;
typedef enum { PREPARE_SUCCESS, PREPARE_UNRECOGNIZED_STATEMENT } PrepareResult;
typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

typedef struct {
  StatementType type;
} Statement;

void print_prompt() { printf("db > "); }

void read_input(InputBuffer* input_buffer) {
  // getline reads a line from stream (in this case stdin)
  ssize_t bytes_read =
    getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

    if (bytes_read <= 0) {
      printf("Error reading input\n");
      exit(EXIT_FAILURE);
    }

   // ignore trailing new line
   input_buffer->input_length = bytes_read - 1;
   input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer* input_buffer) {
  // we need to free `buffer` too because it was malloc'd by newline()'s first argument
  free(input_buffer->buffer);
  free(input_buffer);
}

InputBuffer* new_input_buffer() {
  // malloc assigns input_buffer to heap
  // Needed here because otherwise the memory is freed after this function is called.
  // But the way it is now, allows the setter to be its own function, rather than writing it in main()
  InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    close_input_buffer(input_buffer);
    exit(EXIT_SUCCESS);
  }
  return META_COMMAND_UNRECOGNIZED_COMMAND;
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
  if (strcmp(input_buffer->buffer, "select") == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    statement->type = STATEMENT_INSERT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement* statement) {
  switch (statement->type) {
    case (STATEMENT_SELECT):
      printf("Selecting from database...\n");
      break;
    case (STATEMENT_INSERT):
      printf("Inserting to database...\n");
      break;
  }
}

int main(int argc, char** argv) {
  InputBuffer* input_buffer = new_input_buffer();

  // REPL
  while (true) {
    print_prompt();
    read_input(input_buffer);

    // is valid meta command?
    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer)) {
        case (META_COMMAND_SUCCESS):
          continue;
        case (META_COMMAND_UNRECOGNIZED_COMMAND):
          printf("Unrecognized command '%s'\n", input_buffer->buffer);
          continue;
      }
    }

    // convert input to bytecode for sqlite to process as sql statement
    Statement statement;
    // TODO why is it &statement and not *statement?
    switch (prepare_statement(input_buffer, &statement)) {
      case (PREPARE_SUCCESS):
        break;
      case (PREPARE_UNRECOGNIZED_STATEMENT):
        printf("Unrecognized keyword at start of '%s' .\n", input_buffer->buffer);
        continue;
    }

    execute_statement(&statement);
    printf("Executed.\n");
  }
}
