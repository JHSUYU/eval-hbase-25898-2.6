// Autogenerated Jamon proxy
// /Users/lizhenyu/Desktop/Evaluation/hbase-25898/hbase-server/src/main/jamon/org/apache/hadoop/hbase/tmpl/master/RegionVisualizerTmpl.jamon

package org.apache.hadoop.hbase.tmpl.master;


@org.jamon.annotations.Template(
  signature = "16641E4377F3F072614EBF98F7B441F9")
public class RegionVisualizerTmpl
  extends org.jamon.AbstractTemplateProxy
{
  
  public RegionVisualizerTmpl(org.jamon.TemplateManager p_manager)
  {
     super(p_manager);
  }
  
  protected RegionVisualizerTmpl(String p_path)
  {
    super(p_path);
  }
  
  public RegionVisualizerTmpl()
  {
     super("/org/apache/hadoop/hbase/tmpl/master/RegionVisualizerTmpl");
  }
  
  public interface Intf
    extends org.jamon.AbstractTemplateProxy.Intf
  {
    
    void renderNoFlush(final java.io.Writer jamonWriter) throws java.io.IOException;
    
  }
  public static class ImplData
    extends org.jamon.AbstractTemplateProxy.ImplData
  {
  }
  @Override
  protected org.jamon.AbstractTemplateProxy.ImplData makeImplData()
  {
    return new ImplData();
  }
  @Override public ImplData getImplData()
  {
    return (ImplData) super.getImplData();
  }
  
  
  @Override
  public org.jamon.AbstractTemplateImpl constructImpl(Class<? extends org.jamon.AbstractTemplateImpl> p_class){
    try
    {
      return p_class
        .getConstructor(new Class [] { org.jamon.TemplateManager.class, ImplData.class })
        .newInstance(new Object [] { getTemplateManager(), getImplData()});
    }
    catch (RuntimeException e)
    {
      throw e;
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  protected org.jamon.AbstractTemplateImpl constructImpl(){
    return new RegionVisualizerTmplImpl(getTemplateManager(), getImplData());
  }
  public org.jamon.Renderer makeRenderer()
  {
    return new org.jamon.AbstractRenderer() {
      @Override
      public void renderTo(final java.io.Writer jamonWriter)
        throws java.io.IOException
      {
        render(jamonWriter);
      }
    };
  }
  
  public void render(final java.io.Writer jamonWriter)
    throws java.io.IOException
  {
    renderNoFlush(jamonWriter);
    jamonWriter.flush();
  }
  public void renderNoFlush(final java.io.Writer jamonWriter)
    throws java.io.IOException
  {
    Intf instance = (Intf) getTemplateManager().constructImpl(this);
    instance.renderNoFlush(jamonWriter);
    reset();
  }
  
  
}
