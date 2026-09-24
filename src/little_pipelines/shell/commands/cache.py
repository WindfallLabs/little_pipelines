"""
Cache commands for the Little Pipelines shell.
"""

from __future__ import annotations


class CacheCommands:
    """
    Cache inspection and maintenance commands.

    Expected shell attributes:

        self.cache
        self.console
        self.logger
    """

    # ======================================================================
    # Helpers

    def _list_cache(
        self,
        show_all: bool = False,
    ) -> list[str]:
        """
        Return cache keys.

        Parameters
        ----------
        show_all:
            Include internal cache entries.
        """

        if show_all:

            keys = sorted(
                self.cache.keys()
            )

        else:

            keys = sorted(
                k
                for k in self.cache.keys()
                if not k.endswith("_hashes")  # TODO: Remove
            )

        return keys

    # ======================================================================
    # Inspection

    def _list_cache(self, inp: str):
        """
        Return formatted cache key list.

        Preserves Shell API for tests and backwards compatibility.
        """

        if inp == "--all":
            cached_names = self.cache.keys()
        else:
            cached_names = [
                k
                for k in self.cache.keys()
                if not k.endswith("_hashes")
            ]

        clist = []

        for key in cached_names:
            clist.append(
                f"- '{key}'"
            )

        clist.append(
            f"[bright_black]Total: {len(cached_names)}[/]"
        )

        return clist

    def do_list_cache(self, inp=""):

        for msg in self._list_cache(inp):
            self.console.print(msg)

    def do_cache(
        self,
        inp: str = "",
    ) -> None:
        """
        Alias for list_cache.
        """

        self.do_list_cache(inp)

    # ======================================================================
    # Maintenance

    def do_clear_cache(
        self,
        inp: str,
    ) -> None:
        """
        Clear cached results.

        Examples
        --------

            clear-cache SomeTask

            clear-cache .

        Notes
        -----

        '.' clears the entire cache.
        """

        target = inp.strip()

        if not target:

            self.logger.shell_fail(
                "Task name required "
                "or use '.'"
            )

            return

        # ==========================================================
        # Entire cache

        if target.startswith("."):

            count = len(
                self.cache.keys()
            )

            self.logger.shell_info(
                "Clearing all cached data..."
            )

            with self.console.status(
                "Clearing cache..."
            ):
                self.cache.clear()

            self.logger.shell_info(
                f"Cleared {count} "
                f"cached result(s)."
            )

            return

        # ==========================================================
        # Single task

        self.logger.shell_info(
            f"Clearing cache for "
            f"{target}..."
        )

        self.cache.clear(
            target
        )

        self.logger.shell_info(
            "Complete."
        )

    def do_clear(
        self,
        inp: str,
    ) -> None:
        """
        Alias for clear_cache.
        """

        self.do_clear_cache(inp)
