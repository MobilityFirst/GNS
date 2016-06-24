## GNS Website

This is the website for the Global Name Service (GNS). It is based on the [Jekyll Documentation Theme](http://idratherbewriting.com/documentation-theme-jekyll/) ([Github](https://github.com/tomjohnson1492/documentation-theme-jekyll))

### Local Setup
If you want to be able to render the locally follow 

1. Install dependencies
  * gem (Ruby Gems)
  * nodejs and npm
  * bundler
2. Download the repo and checkout the gh-pages branch
3. Run `bundler install` from the cli in the root directory of the project
4. Run `bundler exec jekyll serve` to start the web server
5. View your local site at http://localhost:4000/GNS/

## Editing content
For advanced instructions on editing content see the [Jekyll Documentation Theme site](http://idratherbewriting.com/documentation-theme-jekyll/).

Markdown files for documentation can be found in the `documentation` folder. Be sure to include the YAML Front Matter block (enclosed by three hyphens) at the beginning of the Markdown file, as this is what ensures the page is rendered on the site and includes important information like the page title and permalink.

To create a link to other pages in the wiki use this format: `{{ site.baseurl }}/other_page_permalink/`. To link to a page called "Other Page" I would write `[Other Page Link Title]({{ site.baseurl }}/other_page_permalink/)`
