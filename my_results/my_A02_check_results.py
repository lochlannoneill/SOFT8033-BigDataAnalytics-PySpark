# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------


# ------------------------------------------
# IMPORTS
# ------------------------------------------
import sys
import codecs


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(my_file_1, my_file_2):
    # 1. We create the output variable
    res = True

    # 2. We open both files
    my_input_stream_1 = codecs.open(my_file_1, "r", encoding="utf-8")
    my_input_stream_2 = codecs.open(my_file_2, "r", encoding="utf-8")

    # 3. We read the full content of each file, removing any empty lines and spaces
    content_1 = [ line.strip().replace(" ", "") for line in my_input_stream_1 if line ]
    content_2 = [ line.strip().replace(" ", "") for line in my_input_stream_2 if line ]

    # 4. We close the files
    my_input_stream_1.close()
    my_input_stream_2.close()

    # 5. We check that both files are equal
    size_1 = len(content_1)

    # 5.1. If both files have the same length
    if (size_1 == len(content_2)):
        # 5.1.1. We compare them line by line
        for index in range(size_1):
            if (content_1[index] != content_2[index]):
                res = False
                break

    # 5.2. If the files have different lengths then they are definitely not equal
    else:
        res = False

    # 6. We return res
    return res


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    PARTS = 3
    def check_part(part):
        assignment_solutions_directory = f"./Assignment_Solutions/A02_Part{part}/result.txt"
        student_solutions_directory = f"./Student_Solutions/A02_Part{part}/result.txt"
        res = my_main(assignment_solutions_directory, student_solutions_directory)
        print(f"Part {part}: {res}")

    if len(sys.argv) > 1:
        part = int(sys.argv[1])
        check_part(part)
    else:
        print(f"Lochlann woz ere")
        print(f"Student_Solutions == Assignment_Solutions")
        for part in range(1, PARTS+1):
            check_part(part)
