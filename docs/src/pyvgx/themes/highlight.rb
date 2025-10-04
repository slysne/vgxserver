require 'rouge' unless defined? ::Rouge.version

module Rouge; module Themes
  class Custom < CSSTheme
    name 'custom'
    style Comment,           fg: '#bbbbbb', italic: true
    style Error,             fg: '#a61717', bg: '#e3d2d2'
    style Str,               fg: '#00ff00'
    style Str::Char,         fg: '#00ff00'
    style Num,               fg: '#00ff00'
    style Keyword,           fg: '#d940ff', bold: true
    style Operator::Word,    fg: '#ffffff', bold: true
    style Punctuation,       fg: '#ffffff', bold: true
    style Name::Tag,         fg: '#000080', bold: true
    style Name::Attribute,   fg: '#ff0000'
    style Generic::Deleted,  fg: '#000000', bg: '#ffdddd', inline_block: true, extend: true
    style Generic::Inserted, fg: '#000000', bg: '#ddffdd', inline_block: true, extend: true
    style Text, {}
  end
end; end


