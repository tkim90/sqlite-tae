#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
  char *buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;
typedef enum {
  PREPARE_SUCCESS,
  PREPARE_SYNTAX_ERROR,
  PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;
typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

// Called "preprocessor directives": performs text replacement before code
// compiles. aka every instance of COLUMN_USERNAME_SIZE is replaced with 32 this
// way, it doesn't take up memory in your program; the final code is just the
// values
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE];
  char email[COLUMN_EMAIL_SIZE];
} Row;

typedef struct {
  StatementType type;
  Row row_to_insert;
} Statement;

/*
 * TLDR: this is a utility macro to get the byte size of a specific attribute
 * within struct without needing to create an actual instance of the struct.
 *
 * "hey compiler, if this were a real Struct pointer, I want to access
 * Attribute"
 *
 * Breakdown:
 * - Struct*                    creates a pointer
 * - (Struct*)0                 creates a NULL pointer by casting a 0, since 0
 * is not a valid memory address
 * - ((Struct *)0)->Attribute   gets the Attribute type info from Struct through
 * a NULL pointer
 *
 * */
#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);             // 4 bytes
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username); // 32 bytes
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);       // 255 bytes
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

/*
 * SERIALIZE & DESERIALIZE
 *
 * void* (void pointer) is a pointer to a generic type (any)
 * generally unsafe but typically used in serialization
 *
 * I should still carefully type cast this at runtime
 *
 * If `destination` were to accept 1 of 2 types, we could use:
 *   1. (Struct*)0    creates a NULL pointer. By casting a 0 to a function
 *     overloading ->
 *     serialize_row_int(Row* source, int* destination) or
 *     serialize_row_double(Row* source, double* destination)
 *
 *   2. Tagged Union (Discriminated Union)
 *     typedef enum {
 *       DESTINATION_INT,
 *       DESTINATION_DOUBLE,
 *     } DestinationType;
 *
 *     typedef struct {
 *       DestinationType type;
 *       union {
 *         int* destination_int;
 *         double* destination_double;
 *       } destination
 *     } Destination;
 */
void serialize_row(Row *source, void *destination) {
  /*
   * memcp copies n chars from src to dest.
   *
   * void *memcpy(void *dst, const void *src, size_t n)
   * ^^ notice the `const void *src` syntax
   * `const` is a hint that *src shouldn't be modified.
   *
   *
   * memcpy(destination address, source address to copy bytes from, number of
   * bytes to copy)
   */
  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

// Table structure that points to pages of rows and keeps tracks of how many
// rows there are in prepare_statement

// 4KB; equivalent to a page in most VMs.
// This way 1 page in our DB is 1 page in the OS, so it's treated as a single
// unit
const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
  uint32_t num_rows;
  void *pages[TABLE_MAX_PAGES]; // array of pointers
} Table;

void print_prompt() { printf("db > "); }

void remove_newline(InputBuffer *input_buffer, ssize_t bytes_read) {
  // removes the \n newline char after processing the input from the repl.
  // removes it from total length and deletes the \n character by replacing w 0.
  input_buffer->input_length = bytes_read - 1;
  input_buffer->buffer[bytes_read - 1] = 0;
}

void read_input(InputBuffer *input_buffer) {
  // getline reads a line from stream (in this case stdin)
  ssize_t bytes_read =
      getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }

  remove_newline(input_buffer, bytes_read);
}

void close_input_buffer(InputBuffer *input_buffer) {
  // we need to free `buffer` too because it was malloc'd by newline()'s first
  // argument
  free(input_buffer->buffer);
  free(input_buffer);
}

InputBuffer *new_input_buffer() {
  // malloc assigns input_buffer to heap
  // Needed here because otherwise the memory is freed after this function is
  // called. But the way it is now, allows the setter to be its own function,
  // rather than writing it in main()
  InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}

MetaCommandResult do_meta_command(InputBuffer *input_buffer) {
  bool is_exit_command = strcmp(input_buffer->buffer, ".exit") == 0;
  if (is_exit_command) {
    close_input_buffer(input_buffer);
    exit(EXIT_SUCCESS);
  }
  return META_COMMAND_UNRECOGNIZED_COMMAND;
}

PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement) {
  if (strcmp(input_buffer->buffer, "select") == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    statement->type = STATEMENT_INSERT;
    int args_assigned = sscanf(
        input_buffer->buffer, "insert %d %s %s", &(statement->row_to_insert.id),
        statement->row_to_insert.username, statement->row_to_insert.email);
    if (args_assigned < 3) {
      return PREPARE_SYNTAX_ERROR;
    }
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

// Figures out where to read/write a particular row in memory
void *read_slot(Table *table, uint32_t row_num) {
  // Calculate which page contains this row
  uint32_t page_num = row_num / ROWS_PER_PAGE;

  // If the page doesn't exist yet, create it
  if (table->pages[page_num] == NULL) {
    table->pages[page_num] = malloc(PAGE_SIZE);
  }

  // Get a pointer to the start of the page
  void *page_start = table->pages[page_num];

  /*
   * get row position by doing modulo %
   * it's perfect for getting a "position"
   *
   * say ROWS_PER_PAGE = 10.
   * if row_num is 0-9, % 10 will be those numbers (0-9)
   * if row_num is 11, % 10 = 1 (first position in next page)
   * if row_num is 15, % 10 = 5 (fifth position in next page)
   * if row_num is 25, % 10 = 5 (fifth position in third page)
   *
   * So row_position_in_page always gives you the row's position within its specific page,
   * regardless of which page it's on.
   */
  uint32_t row_position_in_page = row_num % ROWS_PER_PAGE;

  // `row_position_in_page * ROW_SIZE` calculates how many bytes you need to
  // move from the start of the page to its specific row
  void *row_location = page_start + (row_position_in_page * ROW_SIZE);

  return row_location;
}

void execute_statement(Statement *statement) {
  switch (statement->type) {
  case (STATEMENT_SELECT):
    printf("Selecting from database...\n");
    break;
  case (STATEMENT_INSERT):
    printf("Inserting to database...\n");
    break;
  }
}

int main(int argc, char **argv) {
  InputBuffer *input_buffer = new_input_buffer();

  // REPL
  while (true) {
    print_prompt();
    read_input(input_buffer);

    bool is_valid_meta_command = input_buffer->buffer[0] == '.';
    if (is_valid_meta_command) {
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
    // reminder: &statement CREATES a pointer to statement (gets memory address)
    switch (prepare_statement(input_buffer, &statement)) {
    case (PREPARE_SUCCESS):
      break;
    case (PREPARE_UNRECOGNIZED_STATEMENT):
      printf("Unrecognized keyword at start of '%s' .\n", input_buffer->buffer);
      continue;
    default:
      printf("Syntax error.");
      break;
    }

    execute_statement(&statement);
    printf("Executed.\n");
  }
}
