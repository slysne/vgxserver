#
#
# sudo apt-get install ruby-full
# sudo gem install asciidoctor-pdf
# sudo gem install rouge
# ./makeall.sh 

TABLE_STRIPES="table-stripes=even"
THEME="pdf-theme=themes/pyvgx-theme.yml"
MATH="asciidoctor-mathematical"
FONTS="pdf-fontsdir=themes/fonts;GEM_FONTS_DIR"
ICONS="icons=font"
HIGHLIGHT="source-highlighter=rouge"
HIGHSTYLE="rouge-style=custom"
OUTPUT="PDF"

asciidoctor-pdf --trace -r ./themes/highlight.rb -r $MATH -R . -D $OUTPUT -a $TABLE_STRIPES -a $THEME -a $FONTS -a $ICONS -a $HIGHLIGHT -a $HIGHSTYLE '**/[^_]*.adoc'
