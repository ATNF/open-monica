import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.logging.log4j.core.config.plugins.processor.PluginCache;

/**
 * Build-time helper for the "fullcream" target. Merges the META-INF files which would otherwise collide when several
 * dependency jars are unpacked into a single jar:
 * <ul>
 * <li>META-INF/services/* - provider lists are concatenated (e.g. java.sql.Driver from both JDBC drivers).
 * <li>Log4j2Plugins.dat - log4j plugin caches are merged using log4j-core's own PluginCache.
 * <li>Top-level META-INF LICENSE/NOTICE/DEPENDENCIES files - copied to META-INF/licenses/&lt;jar name&gt;/ so none are
 * lost.
 * </ul>
 * The fullcream target excludes the original copies of these files from the dependency jars.
 *
 * Usage: MergeJarMetadata &lt;output dir&gt; &lt;jar path list, separated by File.pathSeparator&gt;
 */
public class MergeJarMetadata {
  private static final String SERVICES = "META-INF/services/";
  private static final String PLUGIN_CACHE = "META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat";

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: MergeJarMetadata <output dir> <jar path list>");
      System.exit(1);
    }
    File outDir = new File(args[0]);
    Map<String, Set<String>> services = new TreeMap<String, Set<String>>();
    List<URL> pluginCaches = new ArrayList<URL>();

    for (String jarPath : args[1].split(File.pathSeparator)) {
      File jar = new File(jarPath);
      try (JarFile jf = new JarFile(jar)) {
        for (Enumeration<JarEntry> e = jf.entries(); e.hasMoreElements();) {
          JarEntry entry = e.nextElement();
          String name = entry.getName();
          if (entry.isDirectory()) {
            continue;
          }
          if (name.startsWith(SERVICES) && name.indexOf('/', SERVICES.length()) == -1) {
            Set<String> providers = services.get(name);
            if (providers == null) {
              providers = new LinkedHashSet<String>();
              services.put(name, providers);
            }
            for (String line : new String(readAll(jf.getInputStream(entry)), StandardCharsets.UTF_8).split("\r?\n")) {
              line = line.replaceAll("#.*", "").trim();
              if (!line.isEmpty()) {
                providers.add(line);
              }
            }
          } else if (name.equals(PLUGIN_CACHE)) {
            pluginCaches.add(new URL("jar:" + jar.toURI() + "!/" + name));
          } else if (isLicenseFile(name)) {
            File dest = new File(outDir, "META-INF/licenses/" + jar.getName() + "/" + name.substring("META-INF/".length()));
            dest.getParentFile().mkdirs();
            Files.write(dest.toPath(), readAll(jf.getInputStream(entry)));
          }
        }
      }
    }

    for (Map.Entry<String, Set<String>> s : services.entrySet()) {
      File dest = new File(outDir, s.getKey());
      dest.getParentFile().mkdirs();
      try (PrintWriter pw = new PrintWriter(dest, "UTF-8")) {
        for (String provider : s.getValue()) {
          pw.println(provider);
        }
      }
      System.out.println("Merged " + s.getKey() + ": " + s.getValue());
    }

    if (!pluginCaches.isEmpty()) {
      PluginCache cache = new PluginCache();
      cache.loadCacheFiles(Collections.enumeration(pluginCaches));
      File dest = new File(outDir, PLUGIN_CACHE);
      dest.getParentFile().mkdirs();
      try (OutputStream os = new FileOutputStream(dest)) {
        cache.writeCache(os);
      }
      int count = 0;
      for (Map<String, ?> category : cache.getAllCategories().values()) {
        count += category.size();
      }
      System.out.println("Merged " + pluginCaches.size() + " log4j plugin caches (" + count + " plugins)");
    }
  }

  /** True for LICENSE/NOTICE style files directly under META-INF. */
  private static boolean isLicenseFile(String name) {
    if (!name.startsWith("META-INF/") || name.indexOf('/', "META-INF/".length()) != -1) {
      return false;
    }
    String base = name.substring("META-INF/".length()).toUpperCase();
    return base.startsWith("LICENSE") || base.startsWith("NOTICE") || base.startsWith("DEPENDENCIES");
  }

  private static byte[] readAll(InputStream in) throws IOException {
    try (InputStream is = in) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      byte[] buf = new byte[8192];
      int n;
      while ((n = is.read(buf)) != -1) {
        bos.write(buf, 0, n);
      }
      return bos.toByteArray();
    }
  }
}
