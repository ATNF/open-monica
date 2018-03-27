package atnf.atoms.mon.archiver.influx;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TagExtractor {
	private static final String REGEX_DOT_OR_END = "($|[._])";
	private static final String REGEX_DOT_OR_START = "(^|[._])";

	public static class Holder {
		private String s;

		@SuppressWarnings("unused")
		public Holder(String s) {
			super();
			this.s = s;
		}

		public String get() {
			return s;
		}

		public void set(String s) {
			this.s = s;
		}
	}
	
	private final String replace;
	private final Pattern pattern;
	private final Pattern tvPattern;
	private final String tvReplace;

	@SuppressWarnings("unused")
	private TagExtractor(String pattern, String replace, String tagValuePattern, String tagValueReplace) {
		this.pattern = Pattern.compile(REGEX_DOT_OR_START + "(" + pattern + ")" + REGEX_DOT_OR_END);
		this.replace = replace;
		this.tvPattern = Pattern.compile(tagValuePattern);
		this.tvReplace = tagValueReplace;
	}

	@SuppressWarnings("unused")
	private TagExtractor(String pattern) { this(pattern, "", "", "");
	}

	@SuppressWarnings("unused")
	private TagExtractor(String pattern, String replace) { this(pattern, replace, "", "");
	}

	public String  apply(Holder name) {
		Matcher m = pattern.matcher(name.get());
		if (m.find()) {
			//String tagAndSeps = m.group();
			String lastSep = m.group(3);
			String firstSep = m.group(1);
			String sep = (lastSep.isEmpty() || firstSep.isEmpty())? "": 
				((lastSep.contains(".") || firstSep.contains("."))?".":"_");
			String ret = m.group(2);
			if (!(tvReplace.isEmpty() || tvReplace.startsWith("-"))) {
				Matcher m3 = tvPattern.matcher(ret);
				if (m3.find()) {
					ret = m3.replaceAll(tvReplace);
				}
			}
			if (replace.isEmpty() || replace.startsWith("-")) {
				name.set(m.replaceAll(sep));
			} else {
				name.set(m.replaceAll(firstSep + replace + lastSep));
			}
			return ret;
		} else {
			return null;
		}
	}
	
	public static TagExtractor fromString(String s) {
		String[] parts = s.split(" ");
		if (parts.length > 3) {
			return new TagExtractor(parts[0], parts[1], parts[2], parts[3]);
		}
		else if (parts.length > 1) {
			return new TagExtractor(parts[0], parts[1]);
		}
		else {
			return new TagExtractor(parts[0]);
		}
	}
	
}
