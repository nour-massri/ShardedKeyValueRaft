import subprocess, argparse, threading
from pathlib import Path

def parse_args():
    parser = argparse.ArgumentParser(
        prog='ParallelTester',
        description='Test helper for MIT 6.5840 distributed systems class, made to run tests many times')

    parser.add_argument('--testexpr', action="store", default=None, help="The expression to hand to the go tester to filter test cases")
    parser.add_argument('--race', action="store_true", help="Run the tester with the race detector")
    parser.add_argument('--count', action="store", type=int, default=100, help="How many times do we want to run the testcases")
    parser.add_argument('--parallel', action="store", type=int, default=1, help="How many tester instances do we want to run on the machine in parallel")
    parser.add_argument('--outputdir', action="store", type=str, default=".", help="Where to store output files")

    return parser.parse_args()

def construct_process_expr(testexpr, race):
    expr = ["go", "test"]
    if testexpr is not None:
        expr += ["--run", testexpr]
    if race:
        expr += ["--race"]
    return expr

def distribute(count, parallel):
    base, extra = divmod(count, parallel)
    return [base + (i < extra) for i in range(parallel)]

def run_test_thread(process_expr, test_expr, me, parcount, total, outputdir):
    for iteration in range(total):
        testnum = me + iteration * parcount
        output =  subprocess.run(process_expr, stdout=subprocess.PIPE, stderr =subprocess.PIPE)
        outputfile = Path(outputdir).joinpath(f"{testnum}_{test_expr}.txt")
        with open(outputfile, "wb+") as f:
            f.write(output.stdout)

        if output.returncode != 0:
            print(f"Thread {me}: Finished execution of test {testnum} with exit code {output.returncode}. Stored result in {outputfile}")            
        else:
            print(f"Thread {me}: Finished execution of test {testnum}: \u2705")

def main():
    args = parse_args()
    process_expr = construct_process_expr(args.testexpr, args.race)
    distribution = distribute(args.count, args.parallel)

    print("Threads will be run with: ", " ".join(process_expr))
    threads = []
    for paridx in range(args.parallel):
        thread = threading.Thread(target=run_test_thread, args=[process_expr, args.testexpr, paridx, args.parallel, distribution[paridx], args.outputdir])
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

if __name__ == '__main__':
    main()