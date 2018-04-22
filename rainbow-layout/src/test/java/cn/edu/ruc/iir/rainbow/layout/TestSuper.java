package cn.edu.ruc.iir.rainbow.layout;

public class TestSuper
{
    static class A
    {
        int a = 0;
        Object b = new Object();
        public void setA (int a)
        {
            this.a = a;
        }

    }

    static class B extends A
    {
        public void setA (int a)
        {
            this.a = a;
            System.out.println("" + (this.a == super.a));
            System.out.println(this.b == super.b);
        }
        public int getA ()
        {
            return super.a;
        }
    }

    public static void main(String[] args)
    {
        B b = new B();
        b.setA(1);
        System.out.println(b.getA());
    }
}
