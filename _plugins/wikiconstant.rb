
module JekyllJsonball
  class JsonballTag < Liquid::Tag

    def initialize(tag_name, text, tokens)
      super
      @text = text
    end

    def render(context)
    	site = context.registers[:site]
		return site.data["wiki_constants"][@text.strip]
	end
  end

  class JsonballTag2 < Liquid::Tag

    def initialize(tag_name, text, tokens)
      super
      @text = text
    end

    def render(context)
    	site = context.registers[:site]
		return site.data["wiki_samples"][@text.strip]
	end
  end
end

Liquid::Template.register_tag('wikiconstant', JekyllJsonball::JsonballTag)
Liquid::Template.register_tag('wikisample', JekyllJsonball::JsonballTag2)