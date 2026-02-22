<div align = "center">

# CHANGELOG

</div>

<div align = "justify">

All notable changes to this *PostgreSQL DB Management* project will be documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to [`semver`](https://semver.org/) styling.

## Release Note(s)

The release notes are documented, the list of changes to each different release are documented. The `major.minor` are indicated
under `h3` tags, while the `patch` and other below identifiers are listed under `h4` and subsequent headlines. The legend for
changelogs are provided in the detail pane, while the version wise note is as available below.

<details>
<summary>Click Here to View Legend</summary>

<p><small>
<ul style = "list-style-type:circle">
  <li>✨ - <b>Major Feature</b> : something big that was not available before.</li>
  <li>🎉 - <b>Feature Enhancement</b> : a miscellaneous minor improvement of an existing feature.</li>
  <li>🛠️ - <b>Patch/Fix</b> : something that previously didn't work as documented should now work.</li>
  <li>🐛 - <b>Bug/Fix</b> : a bug in the code was resolved and documented.</li>
  <li>⚙️ - <b>Code Efficiency</b> : an existing feature now may not require as much computation or memory.</li>
  <li>💣 - <b>Code Refactoring</b> : a breakable change often associated with `major` version bump.</li>
</ul>
</small></p>

</details><br>

### Anemo Venti `v1` Release Notes

The first stable version focuses on providing a backbone structure to integrate the microservices available in AivenIO and
to provide CI/CD functionalities to auto update data based on a selected interval or other relevant triggers.

#### Release `v1.1.0` | 2026.02.22

Minor bug fixes and patched existing functionalities. Workflow is tested and passing, the details of the version are as
follows:

  * ⚙️ Created a database controller object that handles all types of database operation as per requirement,
  * ✨ Provided the ability to switch between `SQLAlchemy` context manager and `pandas` to fetch or insert data into the
    underlying table.

#### Release `v1.0.0` | 2026.02.22

Initial stable version that brings in the functionality to auto update the FOREX rates in MacroDB schema. The function is
acquired from ``forexrates`` which can now work separately from the project.

  * ✨ Create function and workflow to get FOREX rates data and update to the underlying table,
  * ⚙️ All the code commits from the repository are added, thus the history of the file is maintained and unrelated histories
    are merged in a separate branch with check completes.

</div>