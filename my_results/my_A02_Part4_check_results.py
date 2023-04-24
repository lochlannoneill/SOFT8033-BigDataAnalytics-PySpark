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

# ----------------------------------------------------
# FUNCTION parse_structured_streaming_batch_content
# ----------------------------------------------------
def parse_structured_streaming_batch_content(batch_lines, key_column_index):
    # 1. We create the output variable
    res = []

    # 2. We create any additional variable needed
    res_dict = {}

    # 3. We process the batch_lines
    for line in batch_lines:
        # 3.1. We get the line as a tuple
        my_tuple = tuple(line.split("|"))

        # 3.2. We get the key from the tuple
        my_key = my_tuple[key_column_index]

        # 3.3. We enter the new tuple in the dictionary
        if (my_key not in res_dict):
            res_dict[my_key] = []
        res_dict[my_key].append( my_tuple )

    # 4. We collect the keys of the dictionary as a list
    my_ordered_keys = sorted(res_dict.keys())

    # 5. We bring the elements back to res
    for selected_key in my_ordered_keys:
        for item in res_dict[selected_key]:
            my_new_line = "|".join(list(item))
            res.append(my_new_line)

    # 6. We return res
    return res

# ---------------------------------------------------------
# FUNCTION parse_spark_structured_streaming_solution_file
# ---------------------------------------------------------
def parse_spark_structured_streaming_solution_file(my_file, initial_intermediate_gap, final_gap, key_column_index):
    # 1. We create the output variable
    res = []

    # 2. We open the file for reading
    my_input_stream = codecs.open(my_file, "r", encoding="utf-8")

    # 3. We create as many auxiliary variables as needed
    num_lines = 0
    num_batches = 0
    batch_line_indexes = []

    # 4. We traverse the lines of the file
    for line in my_input_stream:
        # 4.1. We strip the line and remove any white space on it
        line = line.strip().replace(" ", "")

        # 4.2. If the line is non-empty we consider it
        if (line) and ('+' not in line):
            # 4.2.1. If it is the line representing a batch
            if (line.startswith("Batch:")):
                # I. We edit the line
                line = "Batch " + str(num_batches)

                # II. We mark this line as one containing batches
                batch_line_indexes.append(num_lines)

                # III. We increase the batch index for further ones
                num_batches += 1

            # 4.2.2. We append the line
            res.append(line)
            num_lines += 1

    # 4. We close the file
    my_input_stream.close()

    # 5. We find the batches to remove
    batch_to_remove = []
    if (num_batches > 0):
        batch_to_remove = [ (batch_line_indexes[index] == (batch_line_indexes[index+1] - initial_intermediate_gap)) for index in range(num_batches-1) ]
        batch_to_remove.append( batch_line_indexes[num_batches-1] == num_lines - final_gap )

    # 6. We traverse the batches to remove the desired ones
    for index in range(num_batches-1, -1, -1):
        if (batch_to_remove[index] == True):
            # 6.1. We remove it
            for _ in range(initial_intermediate_gap):
                del res[batch_line_indexes[index] - 1]
            num_lines -= initial_intermediate_gap

    # 7. We rename the batches accordingly
    num_batches = 0
    batch_line_indexes = []
    line_index = 0
    for index in range(num_lines):
        # 7.1. If the batch is valid
        if ("Batch " in res[index]):
            # 7.1.1. We edit its line
            res[ index ] = "Batch " + str(num_batches)
            batch_line_indexes.append(line_index)

            # 7.1.2. We increase the number of valid batches
            num_batches += 1

        # 7.2. If the line is relevant for the content of the batch, we remove the initial and final '|'
        if ("|" in res[index]):
            res[ index ] = res[ index ][1:-1]

        # 7.3. We increase line_index
        line_index += 1

    # 8. We parse each batch
    for index in range(num_batches):
        # 8.1. We get the lines involved in the batch
        start_index = batch_line_indexes[index] + 3

        end_index = num_lines
        if (index < num_batches-1):
            end_index = batch_line_indexes[index + 1] - 1

        batch_size = end_index - start_index

        # 8.2. We collect the lines
        batch_lines = res[start_index:end_index]

        # 8.3. We remove the content from res
        for _ in range(batch_size):
            del res[start_index]

        # 8.4. We process the batch
        new_batch_content = parse_structured_streaming_batch_content(batch_lines, key_column_index)

        # 8.5. We add the content back to res
        for new_line in reversed(new_batch_content):
            res.insert(start_index, new_line)

    # 9. We return res
    return res

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(my_file_1,
            my_file_2,
            initial_intermediate_gap,
            final_gap,
            key_column_index
           ):

    # 1. We create the output variable
    res = True

    # 2. We read the full content of each file, removing any empty lines and spaces
    content_1 = parse_spark_structured_streaming_solution_file(my_file_1, initial_intermediate_gap, final_gap, key_column_index)
    content_2 = parse_spark_structured_streaming_solution_file(my_file_2, initial_intermediate_gap, final_gap, key_column_index)

    # 3. We check that both files are equal
    size_1 = len(content_1)

    # 3.1. If both files have the same length
    if (size_1 == len(content_2)):
        # 3.1.1. We compare them line by line
        for index in range(size_1):
            if (content_1[index] != content_2[index]):
                res = False
                break

    # 3.2. If the files have different lengths then they are definitely not equal
    else:
        res = False

    # 6. Print the final outcome
    print("----------------------------------------------------------")
    if (res == True):
        print("Congratulations, the code passed the test!")
    else:
        print("Sorry, the output of the file is incorrect!")
    print("----------------------------------------------------------")

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We get the input values
    assignment_solution_file = "./Assignment_Solutions/A02_Part4/result.txt"
    student_solution_file = "./Student_Solutions/A02_Part4/result.txt"

    # 1.1. If the program is called from console, we modify the parameters
    if (len(sys.argv) > 1):
        # 1.1.1 We get the student folder path
        assignment_solution_file = sys.argv[1]
        student_solution_file = sys.argv[2]

    # 3. We call to my_main
    my_main(assignment_solution_file,
            student_solution_file,
            4,
            3,
            0
           )
