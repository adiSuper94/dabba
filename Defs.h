#ifndef DEFS_H
#define DEFS_H


#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072
#define DEFAULT_PIPE_SIZE 1000000


enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};

typedef enum { heap, sorted, tree } fType;

unsigned int Random_Generate();


#endif

