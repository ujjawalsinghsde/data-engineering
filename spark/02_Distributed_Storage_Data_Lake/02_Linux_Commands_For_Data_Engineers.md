# Linux Commands For Data Engineers

## Introduction

A Big Data Engineer works on Linux servers very often.

Even if Spark, Hadoop, Kafka, or Airflow are managed by cloud services, logs, scripts, files, and troubleshooting still usually happen in Linux terminals.

This note covers the Linux commands needed for labs and real data engineering work.

## Linux Directory Structure

Linux follows a tree structure.

The topmost directory is `/`.

```text
/
|-- home
|   |-- username
|-- data
|   |-- retail_db
|-- tmp
|-- usr
|-- var
```

In Windows we usually say folder.

In Linux we usually say directory.

## Basic Navigation Commands

### `pwd`

Prints current directory.

```bash
pwd
```

Expected output:

```text
/home/<username>
```

Use when:

- You are not sure where you are.
- You need to write an absolute path.

### `whoami`

Shows current logged-in user.

```bash
whoami
```

Expected output:

```text
<username>
```

### `cd`

Changes directory.

```bash
cd /data
cd ~
cd ..
cd -
cd .
cd ../..
```

Meaning:

| Command | Meaning |
|---|---|
| `cd /data` | Go to `/data` using absolute path |
| `cd ~` | Go to home directory |
| `cd ..` | Go to parent directory |
| `cd -` | Go to previous directory |
| `cd .` | Stay in current directory |
| `cd ../..` | Go two levels up |

## Absolute Vs Relative Path

### Absolute Path

Absolute path starts from root `/`.

Example:

```bash
cd /data/retail_db/orders
```

This works from anywhere because the full path is given.

### Relative Path

Relative path depends on current location.

Example:

```bash
cd ../orders
```

This means:

"From where I am now, go one level up, then enter `orders`."

Common mistake:

```bash
cd /landing
```

This means Linux will search for `landing` directly under root `/`.

If `landing` is inside home directory, use:

```bash
cd landing
```

or:

```bash
cd ~/landing
```

## Listing Files With `ls`

Basic:

```bash
ls
```

Detailed:

```bash
ls -l
```

Sort newest first:

```bash
ls -lt
```

Sort oldest first:

```bash
ls -ltr
```

Reverse alphabetical:

```bash
ls -lr
```

Recursive listing:

```bash
ls -R
```

Show hidden files:

```bash
ls -a
```

Combine options:

```bash
ls -latR /data
```

In `ls -l` output:

```text
-rw-r--r--  1 user group  100 Jul 26 file1
drwxr-xr-x  2 user group 4096 Jul 26 dir1
```

First character:

- `-` means normal file.
- `d` means directory.

Colors may show:

- Blue: directory.
- Green: executable.
- Black/white: normal file.

## Creating Files

### `touch`

Creates an empty file.

```bash
touch file1
```

Also updates timestamp if file already exists.

Common issue:

If `touch` gives permission denied, you may be in a directory where you do not have write permission. Go to home directory:

```bash
cd ~
touch file1
```

## Viewing Files

### `cat`

Shows full file content.

```bash
cat file1
```

Create file from terminal input:

```bash
cat > file2
```

Then type content and press `Ctrl+D` to save.

Append content:

```bash
cat >> file1
```

### `head`

Shows first 10 lines.

```bash
head file1
```

Show first 5 lines:

```bash
head -5 file1
```

### `tail`

Shows last 10 lines.

```bash
tail file1
```

Useful for logs:

```bash
tail -f application.log
```

`tail -f` keeps watching new lines as they are added.

## Creating Directories

Create one directory:

```bash
mkdir dir1
```

Create multiple:

```bash
mkdir dir2 dir3 dir4
```

Create nested directories:

```bash
mkdir -p data/results
```

## Removing Files And Directories

Remove empty directory:

```bash
rmdir dir1
```

If directory is not empty, `rmdir` fails.

Remove file:

```bash
rm file1
```

Remove directory recursively:

```bash
rm -R dir2
```

Be careful:

`rm -R` deletes a directory and everything inside it.

Best practice:

Run `ls` first before deleting.

## Copying Files

Copy file:

```bash
cp file1 file2
```

Copy file into directory:

```bash
cp file1 dir3
```

Copy directory recursively:

```bash
cp -R dir3 dir4
```

## Moving And Renaming

Move file to directory:

```bash
mv file1 dir3
```

Rename file:

```bash
mv file1 new_file_name
```

Move means cut-paste.

## Editing Files With `vi`

Open file:

```bash
vi samplefile
```

Basic steps:

1. Press `i` to enter insert mode.
2. Type content.
3. Press `Esc`.
4. Type `:wq`.
5. Press Enter.

Meaning:

- `w` means write/save.
- `q` means quit.

Quit without saving:

```text
:q!
```

## Permissions

Linux permissions are shown for:

- Owner.
- Group.
- Others.

Permission values:

```text
r = read    = 4
w = write   = 2
x = execute = 1
```

Examples:

```text
7 = 4 + 2 + 1 = rwx
6 = 4 + 2     = rw-
4 = 4         = r--
```

### `chmod`

Give all permissions to everyone:

```bash
chmod 777 file1
```

Give owner `rwx`, group `rw`, others `r`:

```bash
chmod 764 file1
```

Breakdown:

```text
owner  = 7 = rwx
group  = 6 = rw-
others = 4 = r--
```

Common data engineering use:

Use permissions carefully for scripts, shared data files, and output folders.

## Searching With `grep`

Find lines containing text:

```bash
grep PENDING_PAYMENT orders.csv
```

Preview first few matches:

```bash
grep PENDING_PAYMENT orders.csv | head
```

Count matches:

```bash
grep PENDING_PAYMENT orders.csv | wc -l
```

Pipe `|` means:

"Take output of previous command and pass it as input to next command."

## Disk Usage

Show disk usage:

```bash
du -h /data/ujjawalsingh
```

`-h` means human readable, like KB, MB, GB.

## SSH To Gateway Node

From local terminal:

```bash
ssh <username>@<gateway-host>
```

Then enter password.

Windows password paste tip:

In some terminals, `Ctrl+V` does not paste password. Right-click may paste it. Password characters may not display, which is normal.

Security note:

Do not save real passwords in Git notes.

## Useful Retail DB Commands

Course dataset examples:

```bash
ls /data/retail_db
ls /data/retail_db/orders
cat /data/retail_db/orders/*
```

Example order format:

```text
order_id, timestamp, customer_id, order_status
1,2013-07-25 00:00:00.0,11599,CLOSED
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
```

## Common Mistakes

- Using `/landing` instead of `landing`.
- Running `touch` or `mkdir` in a directory without permission.
- Forgetting `-R` when copying/removing directories.
- Using `rm -R` without checking path.
- Confusing local Linux commands with HDFS commands.
- Expecting password characters to show during SSH login.

## Best Practices

- Use `pwd` before running important path-based commands.
- Use absolute paths in scripts for clarity.
- Use relative paths carefully during manual practice.
- Preview data with `head` before processing full file.
- Count records with `wc -l` after filtering.
- Keep raw input and filtered output in separate directories.
- Avoid storing credentials in notes or code.

## Interview Questions

### Beginner Questions

- What is the root directory in Linux?
- What is the difference between absolute and relative path?
- What does `pwd` do?
- What does `ls -l` show?
- What is `chmod 764`?

### Intermediate Questions

- How do you count lines matching a pattern?
- How do you copy a directory recursively?
- How do you monitor logs in real time?
- What is the difference between `>` and `>>`?
- Why should we avoid running `rm -R` carelessly?

### Scenario-Based Questions

Your command `cd /landing` fails but `cd landing` works. Why?

Answer:

`/landing` is an absolute path under root. `landing` is a relative path under current directory. If the folder was created in home, use `cd landing` or `cd ~/landing`.

## Quick Revision

```text
pwd       -> current directory
whoami    -> current user
cd        -> change directory
ls -l     -> detailed listing
touch     -> create empty file
cat       -> view file
head      -> first lines
tail      -> last lines
mkdir     -> create directory
rm -R     -> remove directory recursively
cp -R     -> copy directory recursively
mv        -> move or rename
chmod 764 -> owner rwx, group rw, others r
grep      -> search text
du -h     -> disk usage
```
