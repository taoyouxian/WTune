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

        public void func ()
        {
            System.out.println("func in A.");
        }

        public void func2 ()
        {
            this.func();
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

        public void func ()
        {
            System.out.println("func in B.");
        }

        public void func2 ()
        {
            super.func2();
        }
    }

    public static void main(String[] args)
    {
        B b = new B();
        b.setA(1);
        System.out.println(b.getA());

        A a = new A();
        a.func();
        a.func2();
        b.func();
        b.func2();
    }
}
