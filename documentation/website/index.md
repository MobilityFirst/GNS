---
layout: default
root_nav: devsite-doc-set-nav-active
page_type: landing-page
---



## About this website ##

This website is a sample website

List of available constants: 

{% highlight json%}

{% for constant in site.data.wiki_constants %}
{{ constant[0] }} = {{constant[1]}}
{% endfor %}
{% endhighlight %}


For example, use {% raw %} ``` {{site.data.wiki_constants.GNSSERVER_1LOCAL_PROPERTIES_FILE}}``` {% endraw %}