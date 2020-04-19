# Mirror to OneDrive
The objective of this project is to keep mirrored some directories on OneDrive.

I needed to keep an updated copy of some thousands of files from a Linux server to a OneDrive folder.
Initially I tried to use `rclone`, or a quite popular OneDrive Linux client, but I did not have success, so I wrote my own system.

**No warranties**: this is not a backup system, and I am not responsible for any loss of data. These scripts **will** delete any files that are in the OneDrive folders but not in the local source.

This project is released under the public domain.
Of course its dependencies have their own license.
