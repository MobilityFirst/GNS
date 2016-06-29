# GNS Website

This is the website for the Global Name Service (GNS). It is based on the [Jekyll Documentation Theme](http://idratherbewriting.com/documentation-theme-jekyll/) ([Github](https://github.com/tomjohnson1492/documentation-theme-jekyll))

## Local Setup
To be able to build and preview the site locally you must:

1. Install dependencies
  * gem (Ruby Gems)
  * nodejs and npm
  * bundler
2. Download the repo and checkout the gh-pages branch
3. Run `bundler install` from the cli in the root directory of the project
4. Run `bundler exec jekyll serve` to start the web server
5. View your local site at http://localhost:4000/GNS/

## Adding a page
To add a page you must do two things:
1. In the appropriate folder (i.e. documentation) create the markdown file that includes the necessary YAML Front Matter block at the top of the file, the text enclosed by the triple hyphens.

  Here is an example Front Matter block:
  ```
  ---
  title: "My Example Page"
  last_updated: June 28, 2016
  sidebar: documentation_sidebar
  permalink: /my_example_page_permalink/
  toc: true
  ---
  ```
  * title: The title rendered at the top of the page. The quotation marks are optional but let us include special characters like colons.
  * last_updated: This field will be rendered in the footer.
  * sidebar: The sidebar that this page should appear under. This is used for a consitency check between the title listed in the sidebar and the title of the page. Currently there is only one sidebar: documentation_sidebar
  * permalink: Appended to the site's base url this forms the URL that can be used to access this page. 
  * toc: Set to false to remove the table of contents generated at the beginning of the page. NOTE: Header 1, single #, elements are not included in the TOC, only Header 2 and below.

2. Add the page to the sidebar file so that a link will be rendered in the sidebar. This file can be found at `_data/sidebars/documentation_sidebar.json`. You must include a title and and url field for the sidebar item to be properly rendered. The existing content of the sidebar file should serve as a good example of how to add content.

## Editing content
Markdown files for documentation can be found in the `documentation` folder. Be sure to include the YAML Front Matter block (enclosed by three hyphens) at the beginning of the Markdown file, as this is what ensures the page is rendered on the site and includes important information like the page title and permalink.

To create a link to other pages in the wiki use this format: `{{ site.baseurl }}/other_page_permalink/`. To link to a page called "Other Page" write `[Other Page Link Title]({{ site.baseurl }}/other_page_permalink/)`

For advanced instructions on editing content see the [Jekyll Documentation Theme site](http://idratherbewriting.com/documentation-theme-jekyll/). This site is a fork of this theme with some modifications so keep that in mind while browsing the original content. Many features and files have been removed in order to simplify things and YAML has been replaced with JSON in various places.

## Publishing your changes
Commit and push your changes for them to appear on the public website. It is highly recommended that you preview your changes in a markdown previewer or by running the site locally.

## Editing in Github
Markdown files for the site can be changed in Github's web interface and you can make use of Github's preview function. However each edit will generate a commit, so make sure you preview and proofread your changes to avoid additional commits.
