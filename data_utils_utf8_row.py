# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Description: generate inputs and targets for the DLRM benchmark
#
# Utility function(s) to download and pre-process public data sets
#   - Criteo Kaggle Display Advertising Challenge Dataset
#     https://labs.criteo.com/2014/02/kaggle-display-advertising-challenge-dataset
#   - Criteo Terabyte Dataset
#     https://labs.criteo.com/2013/12/download-terabyte-click-logs
#
# After downloading dataset, run:
#   getCriteoAdData(
#       datafile="<path-to-train.txt>",
#       o_filename=kaggleAdDisplayChallenge_processed.npz,
#       max_ind_range=-1,
#       sub_sample_rate=0.0,
#       days=7,
#       data_split='train',
#       randomize='total',
#       criteo_kaggle=True,
#       memory_map=False
#   )
#   getCriteoAdData(
#       datafile="<path-to-day_{0,...,23}>",
#       o_filename=terabyte_processed.npz,
#       max_ind_range=-1,
#       sub_sample_rate=0.0,
#       days=24,
#       data_split='train',
#       randomize='total',
#       criteo_kaggle=False,
#       memory_map=False
#   )

from __future__ import absolute_import, division, print_function, unicode_literals

import sys
from multiprocessing import Manager, Process

# import os
from os import path

# import io
# from io import StringIO
# import collections as coll

import numpy as np
import time




def processCriteoAdData(
        d_path, 
        d_file, 
        #! MODIFICATION
        npzfile,
        pof_output,
        # data,
        pas_output,
        #! MODIFICATION FINISHES
        i, 
        convertDicts, 
        pre_comp_counts):
    # Process Kaggle Display Advertising Challenge or Terabyte Dataset
    # by converting unicode strings in X_cat to integers and
    # converting negative integer values in X_int.
    #
    # Loads data in the form "{kaggle|terabyte}_day_i.npz" where i is the day.
    #
    # Inputs:
    #   d_path (str): path for {kaggle|terabyte}_day_i.npz files
    #   i (int): splits in the dataset (typically 0 to 7 or 0 to 24)

    # process data if not all files exist
    filename_i = npzfile + "_{0}_processed.npz".format(i)

    #!
    # if path.exists(filename_i):
    #     print("Using existing " + filename_i, end="\n")
    # else:
    #     print("Not existing " + filename_i)
    # with np.load(npzfile + "_{0}.npz".format(i)) as data:
    #!
    ## there is only one dictionary in this list
    print("Length of pof_output ", len(pof_output))
    # for k, data in enumerate(pof_output):
    #     print("enumerate k = ", k, end="\n")
    data = pof_output
    print("length of data: ", len(data))
        # categorical features

    # Approach 2a: using pre-computed dictionaries
    X_cat_t = np.zeros(data["X_cat_t"].shape)
    for j in range(26):
        for k, x in enumerate(data["X_cat_t"][j, :]):
            X_cat_t[j, k] = convertDicts[j][x]
    # continuous features
    X_int = data["X_int"]
    X_int[X_int < 0] = 0

    # add log operation for X_int
    X_int = np.log(X_int.astype(np.float32) + 1)
    # targets
    y = data["y"]
    # np.savez_compressed(
    #     filename_i,
    #     # X_cat = X_cat,
    #     X_cat=np.transpose(X_cat_t),  # transpose of the data
    #     X_int=X_int,
    #     y=y,
    # )
    #! MODIFICATION
    # Initialize the list to store data segments
    pas_data_segment = {
        'X_cat': np.transpose(X_cat_t),  # transpose of the data
        'X_int': X_int,
        'y': y,
    }
    pas_output[i] = pas_data_segment;#.append(pas_data_segment)
    print("length of pas_data_segment X_int", len(pas_data_segment['X_int']))
    print("length of pas_data_segment X_cat", len(pas_data_segment['X_cat']))
    print("length of pas_data_segment y", len(pas_data_segment['y']))
    print("length of pas_output: ", len(pas_output[i]))

    print("Processed " + filename_i, end="\n")
    # sanity check (applicable only if counts have been pre-computed & are re-computed)
    # for j in range(26):
    #    if pre_comp_counts[j] != counts[j]:
    #        sys.exit("ERROR: Sanity check on counts has failed")
    # print("\nSanity check on counts passed")

    return


def concatCriteoAdData(
    d_path,
    d_file,
    npzfile,
    trafile,
    #! MODIFICATION
    pas_output,
    counts,
    #! MODIFICATION FINISHES
    days,
    data_split,
    randomize,
    total_per_file,
    total_count,
    memory_map,
    o_filename,
):
    # Concatenates different days and saves the result.
    #
    # Inputs:
    #   days (int): total number of days in the dataset (typically 7 or 24)
    #   d_path (str): path for {kaggle|terabyte}_day_i.npz files
    #   o_filename (str): output file name
    #
    # Output:
    #   o_file (str): output file path

    if memory_map:
        # dataset break up per fea
        # tar_fea = 1   # single target
        den_fea = 13  # 13 dense  features
        spa_fea = 26  # 26 sparse features
        # tad_fea = tar_fea + den_fea
        # tot_fea = tad_fea + spa_fea
        # create offset per file
        offset_per_file = np.array([0] + [x for x in total_per_file])
        for i in range(days):
            offset_per_file[i + 1] += offset_per_file[i]

        # Approach 4: Fisher-Yates-Rao (FYR) shuffle algorithm
        # 1st pass of FYR shuffle
        # check if data already exists
        recreate_flag = False
        for j in range(days):
            filename_j_y = npzfile + "_{0}_intermediate_y.npy".format(j)
            filename_j_d = npzfile + "_{0}_intermediate_d.npy".format(j)
            filename_j_s = npzfile + "_{0}_intermediate_s.npy".format(j)
            if (
                path.exists(filename_j_y)
                and path.exists(filename_j_d)
                and path.exists(filename_j_s)
            ):
                print(
                    "Using existing\n"
                    + filename_j_y
                    + "\n"
                    + filename_j_d
                    + "\n"
                    + filename_j_s
                )
            else:
                recreate_flag = True
        # reorder across buckets using sampling
        if recreate_flag:
            # init intermediate files (.npy appended automatically)
            for j in range(days):
                filename_j_y = npzfile + "_{0}_intermediate_y".format(j)
                filename_j_d = npzfile + "_{0}_intermediate_d".format(j)
                filename_j_s = npzfile + "_{0}_intermediate_s".format(j)
                np.save(filename_j_y, np.zeros((total_per_file[j])))
                np.save(filename_j_d, np.zeros((total_per_file[j], den_fea)))
                np.save(filename_j_s, np.zeros((total_per_file[j], spa_fea)))
            # start processing files
            total_counter = [0] * days
            for i in range(days):
                filename_i = npzfile + "_{0}_processed.npz".format(i)
                with np.load(filename_i) as data:
                    X_cat = data["X_cat"]
                    X_int = data["X_int"]
                    y = data["y"]
                size = len(y)
                # sanity check
                if total_per_file[i] != size:
                    sys.exit("ERROR: sanity check on number of samples failed")
                # debug prints
                print("Reordering (1st pass) " + filename_i)

                # create buckets using sampling of random ints
                # from (discrete) uniform distribution
                buckets = []
                for _j in range(days):
                    buckets.append([])
                counter = [0] * days
                days_to_sample = days if data_split == "none" else days - 1
                if randomize == "total":
                    rand_u = np.random.randint(low=0, high=days_to_sample, size=size)
                    for k in range(size):
                        # sample and make sure elements per buckets do not overflow
                        if data_split == "none" or i < days - 1:
                            # choose bucket
                            p = rand_u[k]
                            # retry of the bucket is full
                            while total_counter[p] + counter[p] >= total_per_file[p]:
                                p = np.random.randint(low=0, high=days_to_sample)
                        else:  # preserve the last day/bucket if needed
                            p = i
                        buckets[p].append(k)
                        counter[p] += 1
                else:  # randomize is day or none
                    for k in range(size):
                        # do not sample, preserve the data in this bucket
                        p = i
                        buckets[p].append(k)
                        counter[p] += 1

                # sanity check
                if np.sum(counter) != size:
                    sys.exit("ERROR: sanity check on number of samples failed")
                # debug prints
                # print(counter)
                # print(str(np.sum(counter)) + " = " + str(size))
                # print([len(x) for x in buckets])
                # print(total_counter)

                # partially feel the buckets
                for j in range(days):
                    filename_j_y = npzfile + "_{0}_intermediate_y.npy".format(j)
                    filename_j_d = npzfile + "_{0}_intermediate_d.npy".format(j)
                    filename_j_s = npzfile + "_{0}_intermediate_s.npy".format(j)
                    start = total_counter[j]
                    end = total_counter[j] + counter[j]
                    # target buckets
                    fj_y = np.load(filename_j_y, mmap_mode="r+")
                    # print("start=" + str(start) + " end=" + str(end)
                    #       + " end - start=" + str(end - start) + " "
                    #       + str(fj_y[start:end].shape) + " "
                    #       + str(len(buckets[j])))
                    fj_y[start:end] = y[buckets[j]]
                    del fj_y
                    # dense buckets
                    fj_d = np.load(filename_j_d, mmap_mode="r+")
                    # print("start=" + str(start) + " end=" + str(end)
                    #       + " end - start=" + str(end - start) + " "
                    #       + str(fj_d[start:end, :].shape) + " "
                    #       + str(len(buckets[j])))
                    fj_d[start:end, :] = X_int[buckets[j], :]
                    del fj_d
                    # sparse buckets
                    fj_s = np.load(filename_j_s, mmap_mode="r+")
                    # print("start=" + str(start) + " end=" + str(end)
                    #       + " end - start=" + str(end - start) + " "
                    #       + str(fj_s[start:end, :].shape) + " "
                    #       + str(len(buckets[j])))
                    fj_s[start:end, :] = X_cat[buckets[j], :]
                    del fj_s
                    # update counters for next step
                    total_counter[j] += counter[j]

        # 2nd pass of FYR shuffle
        # check if data already exists
        for j in range(days):
            filename_j = npzfile + "_{0}_reordered.npz".format(j)
            if path.exists(filename_j):
                print("Using existing " + filename_j)
            else:
                recreate_flag = True
        # reorder within buckets
        if recreate_flag:
            for j in range(days):
                filename_j_y = npzfile + "_{0}_intermediate_y.npy".format(j)
                filename_j_d = npzfile + "_{0}_intermediate_d.npy".format(j)
                filename_j_s = npzfile + "_{0}_intermediate_s.npy".format(j)
                fj_y = np.load(filename_j_y)
                fj_d = np.load(filename_j_d)
                fj_s = np.load(filename_j_s)

                indices = range(total_per_file[j])
                if randomize == "day" or randomize == "total":
                    if data_split == "none" or j < days - 1:
                        indices = np.random.permutation(range(total_per_file[j]))

                filename_r = npzfile + "_{0}_reordered.npz".format(j)
                print("Reordering (2nd pass) " + filename_r)
                np.savez_compressed(
                    filename_r,
                    X_cat=fj_s[indices, :],
                    X_int=fj_d[indices, :],
                    y=fj_y[indices],
                )

        """
        # sanity check (under no reordering norms should be zero)
        for i in range(days):
            filename_i_o = npzfile + "_{0}_processed.npz".format(i)
            print(filename_i_o)
            with np.load(filename_i_o) as data_original:
                X_cat_o = data_original["X_cat"]
                X_int_o = data_original["X_int"]
                y_o = data_original["y"]
            filename_i_r = npzfile + "_{0}_reordered.npz".format(i)
            print(filename_i_r)
            with np.load(filename_i_r) as data_reordered:
                X_cat_r = data_reordered["X_cat"]
                X_int_r = data_reordered["X_int"]
                y_r = data_reordered["y"]
            print(np.linalg.norm(y_o - y_r))
            print(np.linalg.norm(X_int_o - X_int_r))
            print(np.linalg.norm(X_cat_o - X_cat_r))
        """

    else:
        print("Concatenating multiple days into %s.npz file" % str(d_path + o_filename))

        # load and concatenate data
        for i in range(days):
            # filename_i = npzfile + "_{0}_processed.npz".format(i)
            # with np.load(filename_i) as data:
            print("i = ", i, ", length: ", len(pas_output[i]))
            # for k, data in enumerate(pas_output[i]):
            #     print("k = ", k)
            data = pas_output[i]
            if i == 0:
                X_cat = data["X_cat"]
                X_int = data["X_int"]
                y = data["y"]
            else:
                X_cat = np.concatenate((X_cat, data["X_cat"]))
                X_int = np.concatenate((X_int, data["X_int"]))
                y = np.concatenate((y, data["y"]))
            print("Loaded day:", i, "y = 1:", len(y[y == 1]), "y = 0:", len(y[y == 0]))

        # with np.load(d_path + d_file + "_fea_count.npz") as data:
        #     counts = data["counts"]
        # print("Loaded counts!")
        print("X_cat length: ", len(X_cat))
        print("X_int length: ", len(X_int))
        print("y length: ", len(y))

        # # np.savez_compressed(
        # np.savez(
        #     d_path + o_filename + ".npz",
        #     X_cat=X_cat,
        #     X_int=X_int,
        #     y=y,
        #     counts=counts,
        # )

    return d_path + o_filename + ".npz"



def getCriteoAdData(
    datafile,
    o_filename,
    max_ind_range=-1,
    sub_sample_rate=0.0,
    days=7,
    data_split="train",
    randomize="total",
    criteo_kaggle=True,
    memory_map=False,
    dataset_multiprocessing=False,
):
    # Passes through entire dataset and defines dictionaries for categorical
    # features and determines the number of total categories.
    #
    # Inputs:
    #    datafile : path to downloaded raw data file
    #    o_filename (str): saves results under o_filename if filename is not ""
    #
    # Output:
    #   o_file (str): output file path
    t0 = time.perf_counter()
    # split the datafile into path and filename
    lstr = datafile.split("/")
    d_path = "/".join(lstr[0:-1]) + "/"
    d_file = lstr[-1].split(".")[0] if criteo_kaggle else lstr[-1]
    npzfile = d_path + ((d_file + "_day") if criteo_kaggle else d_file)
    trafile = d_path + ((d_file + "_fea") if criteo_kaggle else "fea")

    # count number of datapoints in training set
    total_file = d_path + d_file + "_day_count.npz"
    if path.exists(total_file):
        with np.load(total_file) as data:
            total_per_file = list(data["total_per_file"])
        total_count = np.sum(total_per_file)
        print("Skipping counts per file (already exist)")
    else:
        total_count = 0
        total_per_file = []
        if criteo_kaggle:
            # WARNING: The raw data consists of a single train.txt file
            # Each line in the file is a sample, consisting of 13 continuous and
            # 26 categorical features (an extra space indicates that feature is
            # missing and will be interpreted as 0).
            if path.exists(datafile):
                print("Reading data from path=%s" % (datafile))
                with open(str(datafile)) as f:
                    for _ in f:
                        total_count += 1
                total_per_file.append(total_count)
                # reset total per file due to split
                num_data_per_split, extras = divmod(total_count, days)
                total_per_file = [num_data_per_split] * days
                for j in range(extras):
                    total_per_file[j] += 1
                # split into days (simplifies code later on)
                file_id = 0
                boundary = total_per_file[file_id]


                # nf = open(npzfile + "_" + str(file_id), "w")
                # with open(str(datafile)) as f:
                #     for j, line in enumerate(f):
                #         if j == boundary:
                #             nf.close()
                #             file_id += 1
                #             nf = open(npzfile + "_" + str(file_id), "w")
                #             boundary += total_per_file[file_id]
                #         nf.write(line)
                # nf.close()
                #! MODIFIED: Instead of opening new files, initialize a list to store all file data in memory.
                sif_output = [[] for _ in range(days)]
                current_file_data = sif_output[file_id]
                with open(str(datafile)) as f:
                    for j, line in enumerate(f):
                        if j == boundary:
                            file_id += 1
                            # MODIFIED: Switch to the next inner list for the new "file" or split of data.
                            current_file_data = sif_output[file_id]
                            boundary += total_per_file[file_id]
                        # MODIFIED: Append line to the in-memory data structure instead of writing to a file.
                        current_file_data.append(line)
                #! MODIFICATION FINISHES

            else:
                sys.exit(
                    "ERROR: Criteo Kaggle Display Ad Challenge Dataset path is invalid; please download from https://labs.criteo.com/2014/02/kaggle-display-advertising-challenge-dataset"
                )
        else:
            # WARNING: The raw data consist of day_0.gz,... ,day_23.gz text files
            # Each line in the file is a sample, consisting of 13 continuous and
            # 26 categorical features (an extra space indicates that feature is
            # missing and will be interpreted as 0).
            for i in range(days):
                datafile_i = datafile + "_" + str(i)  # + ".gz"
                if path.exists(str(datafile_i)):
                    print("Reading data from path=%s" % (str(datafile_i)))
                    # file day_<number>
                    total_per_file_count = 0
                    with open(str(datafile_i)) as f:
                        for _ in f:
                            total_per_file_count += 1
                    total_per_file.append(total_per_file_count)
                    total_count += total_per_file_count
                else:
                    sys.exit(
                        "ERROR: Criteo Terabyte Dataset path is invalid; please download from https://labs.criteo.com/2013/12/download-terabyte-click-logs"
                    )

    # process a file worth of data and reinitialize data
    # note that a file main contain a single or multiple splits
    def process_one_file(
        #! MODIFICATION
        # datfile,
        data_input,
        # npzfile,
        data_output,
        #!
        split,
        num_data_in_split,
        dataset_multiprocessing,
        convertDictsDay=None,
        resultDay=None,
    ):
        if dataset_multiprocessing:
            convertDicts_day = [{} for _ in range(26)]

        #!
        # with open(str(datfile)) as f:
        #!
        y = np.zeros(num_data_in_split, dtype="i4")  # 4 byte int
        X_int = np.zeros((num_data_in_split, 13), dtype="i4")  # 4 byte int
        X_cat = np.zeros((num_data_in_split, 26), dtype="i4")  # 4 byte int
        if sub_sample_rate == 0.0:
            rand_u = 1.0
        else:
            rand_u = np.random.uniform(low=0.0, high=1.0, size=num_data_in_split)

        i = 0
        percent = 0
        #!
        # for k, line in enumerate(f):
        for k, line in enumerate(data_input):
        #!
            # process a line (data point)
            line = line.split("\t")
            # set missing values to zero
            for j in range(len(line)):
                if (line[j] == "") or (line[j] == "\n"):
                    line[j] = "0"
            # sub-sample data by dropping zero targets, if needed
            target = np.int32(line[0])
            if (
                target == 0
                and (rand_u if sub_sample_rate == 0.0 else rand_u[k])
                < sub_sample_rate
            ):
                continue

            y[i] = target
            X_int[i] = np.array(line[1:14], dtype=np.int32)
            if max_ind_range > 0:
                X_cat[i] = np.array(
                    list(map(lambda x: int(x, 16) % max_ind_range, line[14:])),
                    dtype=np.int32,
                )
            else:
                X_cat[i] = np.array(
                    list(map(lambda x: int(x, 16), line[14:])), dtype=np.int32
                )

            # count uniques
            if dataset_multiprocessing:
                for j in range(26):
                    convertDicts_day[j][X_cat[i][j]] = 1
                # debug prints
                if float(i) / num_data_in_split * 100 > percent + 1:
                    percent = int(float(i) / num_data_in_split * 100)
                    print(
                        "Load %d/%d (%d%%) Split: %d  Label True: %d  Stored: %d"
                        % (
                            i,
                            num_data_in_split,
                            percent,
                            split,
                            target,
                            y[i],
                        ),
                        end="\n",
                    )
            else:
                for j in range(26):
                    convertDicts[j][X_cat[i][j]] = 1
                # debug prints
                print(
                    "Load %d/%d  Split: %d  Label True: %d  Stored: %d"
                    % (
                        i,
                        num_data_in_split,
                        split,
                        target,
                        y[i],
                    ),
                    end="\r",
                )
            i += 1

        # store num_data_in_split samples or extras at the end of file
        # count uniques
        # X_cat_t  = np.transpose(X_cat)
        # for j in range(26):
        #     for x in X_cat_t[j,:]:
        #         convertDicts[j][x] = 1
        # store parsed

        # filename_s = npzfile + "_{0}.npz".format(split)
        # if path.exists(filename_s):
        #     print("\nSkip existing " + filename_s)
        # else:
        #     np.savez_compressed(
        #         filename_s,
        #         X_int=X_int[0:i, :],
        #         # X_cat=X_cat[0:i, :],
        #         X_cat_t=np.transpose(X_cat[0:i, :]),  # transpose of the data
        #         y=y[0:i],
        #     )
        #     print("\nSaved " + npzfile + "_{0}.npz!".format(split))
        #! MODIFICATION
        # Initialize the list to store data segments
        pof_data_segment = {
            'X_int': X_int[0:i, :],
            # 'X_cat': X_cat[0:i, :],
            'X_cat_t': np.transpose(X_cat[0:i, :]),  # transpose of the data
            'y': y[0:i],
        }
        data_output[split] = pof_data_segment;#.append(pof_data_segment)
        print("length of pof_data_segment X_int", len(pof_data_segment['X_int']))
        print("length of pof_data_segment X_cat_t", len(pof_data_segment['X_cat_t']))
        print("length of pof_data_segment y", len(pof_data_segment['y']))
        print("length of data_output: ", len(data_output[split]))
        print("\nSaved data segment for {0}!".format(split))
        #! MODIFICATION FINISHES

        if dataset_multiprocessing:
            resultDay[split] = i
            convertDictsDay[split] = convertDicts_day
            return
        else:
            return i

    t1 = time.perf_counter()
    # create all splits (reuse existing files if possible)
    #! DEFAULT TRUE
    # recreate_flag = False
    recreate_flag = True
    #!

    convertDicts = [{} for _ in range(26)]
    #! MODIFICATION
    # pof_output = [[] for _ in range(days)]
    # pas_output = [[] for _ in range(days)]
    # print("Length of pof_output ", len(pof_output))
    # print("Length of pas_output ", len(pas_output))
    #! MODIFICATION FINISHES
    # WARNING: to get reproducable sub-sampling results you must reset the seed below
    # np.random.seed(123)
    # in this case there is a single split in each day
    #! COMMENT
    # for i in range(days):
    #     npzfile_i = npzfile + "_{0}.npz".format(i)
    #     npzfile_p = npzfile + "_{0}_processed.npz".format(i)
    #     if path.exists(npzfile_i):
    #         print("Skip existing " + npzfile_i)
    #     elif path.exists(npzfile_p):
    #         print("Skip existing " + npzfile_p)
    #     else:
    #         recreate_flag = True
    #! COMMENT FINISHES
    pof_output = [{} for _ in range(days)]
    if recreate_flag:
        if dataset_multiprocessing:
            resultDay = Manager().dict()
            convertDictsDay = Manager().dict()
            pofOutput = Manager().dict()
            processes = [
                Process(
                    target=process_one_file,
                    name="process_one_file:%i" % i,
                    args=(
                        # npzfile + "_{0}".format(i),
                        #! MODIFICATION
                        sif_output[i],
                        # npzfile,
                        pofOutput,
                        #! MODIFICATION FINISHES
                        i,
                        total_per_file[i],
                        dataset_multiprocessing,
                        convertDictsDay,
                        resultDay,
                    ),
                )
                for i in range(0, days)
            ]
            for process in processes:
                process.start()
            for process in processes:
                process.join()
            for day in range(days):
                total_per_file[day] = resultDay[day]
                print("Constructing convertDicts Split: {}".format(day))
                convertDicts_tmp = convertDictsDay[day]
                pof_output[day] = pofOutput[day]
                for i in range(26):
                    for j in convertDicts_tmp[i]:
                        convertDicts[i][j] = 1
        else:
            for i in range(days):
                total_per_file[i] = process_one_file(
                    # npzfile + "_{0}".format(i),
                    #! MODIFICATION
                    sif_output[i],
                    # npzfile,
                    pof_output[i],
                    #! MODIFICATION FINISHES
                    i,
                    total_per_file[i],
                    dataset_multiprocessing,
                )

    # for i in range(26):
    #     print("convertDicts[",i,"]",convertDicts[i])

    # print("Length of pof_output ", len(pof_output[0]))
    # print("Length of pas_output ", len(pas_output[0]))

    # report and save total into a file
    total_count = np.sum(total_per_file)
    #! COMMENT
    # if not path.exists(total_file):
    #     np.savez_compressed(total_file, total_per_file=total_per_file)
    #!
    print("Total number of samples:", total_count)
    print("Divided into days/splits:\n", total_per_file)

    t2 = time.perf_counter()
    # dictionary files
    counts = np.zeros(26, dtype=np.int32)
    if recreate_flag:
        # create dictionaries
        for j in range(26):
            for i, x in enumerate(convertDicts[j]):
                convertDicts[j][x] = i
            #! COMMENT
            # dict_file_j = d_path + d_file + "_fea_dict_{0}.npz".format(j)
            # if not path.exists(dict_file_j):
            #     np.savez_compressed(
            #         dict_file_j, unique=np.array(list(convertDicts[j]), dtype=np.int32)
            #     )
            #!
            counts[j] = len(convertDicts[j])
            # print("convertDicts[",j,"]",convertDicts[j])
        # store (uniques and) counts
        #! COMMENT
        # count_file = d_path + d_file + "_fea_count.npz"
        # if not path.exists(count_file):
        #     np.savez_compressed(count_file, counts=counts)
        #! COMMENT FINISHES
    #! COMMNET
    # else:
    #     # create dictionaries (from existing files)
    #     for j in range(26):
    #         with np.load(d_path + d_file + "_fea_dict_{0}.npz".format(j)) as data:
    #             unique = data["unique"]
    #         # print("unique: ", unique)
    #         for i, x in enumerate(unique):
    #             convertDicts[j][x] = i
    #     # load (uniques and) counts
    #     with np.load(d_path + d_file + "_fea_count.npz") as data:
    #         counts = data["counts"]
    #! COMMENT FINISHES

    t3 = time.perf_counter()
    # process all splits
    pas_output = [{} for _ in range(days)]
    if dataset_multiprocessing:
        pasOutput = Manager().dict()
        processes = [
            Process(
                target=processCriteoAdData,
                name="processCriteoAdData:%i" % i,
                args=(
                    d_path,
                    d_file,
                    #! MODIFICATION
                    npzfile,
                    pof_output[i],
                    pasOutput,
                    #! MODIFICATION FINISHES
                    i,
                    convertDicts,
                    counts,
                ),
            )
            for i in range(0, days)
        ]
        for process in processes:
            process.start()
        for process in processes:
            process.join()
        for day in range(days):
            pas_output[day] = pasOutput[day]
    else:
        
        for i in range(days):
            processCriteoAdData(
                d_path, 
                d_file, 
                #! MODIFICATION
                npzfile,
                pof_output[i],
                pas_output[i],
                #! MODIFICATION FINISHES
                i, 
                convertDicts, 
                counts)

    t4 = time.perf_counter()
    o_file = concatCriteoAdData(
        d_path,
        d_file,
        npzfile,
        trafile,
        #! MODIFICATION
        pas_output,
        counts,
        #! MODIFICATION FINISHES
        days,
        data_split,
        randomize,
        total_per_file,
        total_count,
        memory_map,
        o_filename,
    )

    t5 = time.perf_counter()
    print("Splitting Input Files: %s s", (t1-t0))
    print("Process One File: %s s", (t2-t1))
    print("Creating Dictionary: %s s", (t3-t2))
    print("Process All Splits: %s s", (t4-t3))
    print("Concatenating Files: %s s", (t5-t4))
    # print("Total Execution Time: %s s", (t5-t0))

    return o_file


def loadDataset(
    dataset,
    max_ind_range,
    sub_sample_rate,
    randomize,
    data_split,
    raw_path="",
    pro_data="",
    memory_map=False,
    dataset_multiprocessing=False
):
    # dataset
    if dataset == "kaggle":
        days = 64
        o_filename = "kaggleAdDisplayChallenge_processed"
    elif dataset == "terabyte":
        days = 24
        o_filename = "terabyte_processed"
    else:
        raise (ValueError("Data set option is not supported"))

    # split the datafile into path and filename
    lstr = raw_path.split("/")
    d_path = "/".join(lstr[0:-1]) + "/"
    d_file = lstr[-1].split(".")[0] if dataset == "kaggle" else lstr[-1]
    npzfile = (d_file + "_day") if dataset == "kaggle" else d_file
    # trafile = d_path + ((d_file + "_fea") if dataset == "kaggle" else "fea")

    # check if pre-processed data is available
    data_ready = True
    if memory_map:
        for i in range(days):
            reo_data = d_path + npzfile + "_{0}_reordered.npz".format(i)
            if not path.exists(str(reo_data)):
                data_ready = False
    else:
        if not path.exists(str(pro_data)):
            data_ready = False

    # pre-process data if needed
    # WARNNING: when memory mapping is used we get a collection of files
    if data_ready:
        print("Reading pre-processed data=%s" % (str(pro_data)))
        file = str(pro_data)
    else:
        print("Reading raw data=%s" % (str(raw_path)))
        file = getCriteoAdData(
            raw_path,
            o_filename,
            max_ind_range,
            sub_sample_rate,
            days,
            data_split,
            randomize,
            dataset == "kaggle",
            memory_map,
            dataset_multiprocessing
        )

    return file, days


if __name__ == "__main__":
    ### import packages ###
    import argparse

    ### parse arguments ###
    parser = argparse.ArgumentParser(description="Preprocess Criteo dataset")
    # model related parameters
    parser.add_argument("--max-ind-range", type=int, default=-1)
    parser.add_argument("--data-sub-sample-rate", type=float, default=0.0)  # in [0, 1]
    parser.add_argument("--data-randomize", type=str, default="total")  # or day or none
    parser.add_argument("--memory-map", action="store_true", default=False)
    parser.add_argument("--data-set", type=str, default="kaggle")  # or terabyte
    parser.add_argument("--raw-data-file", type=str, default="")
    parser.add_argument("--processed-data-file", type=str, default="")
    parser.add_argument("--dataset-multiprocessing", action="store_true", default=False)
    args = parser.parse_args()

    loadDataset(
        args.data_set,
        args.max_ind_range,
        args.data_sub_sample_rate,
        args.data_randomize,
        "train",
        args.raw_data_file,
        args.processed_data_file,
        args.memory_map,
        args.dataset_multiprocessing
    )
