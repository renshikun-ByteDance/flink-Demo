package operator.beans;

/**
 * POJO对象（Plain Ordinary Java Object），简单普通的java对象
 * 1.必须有无参构造函数
 * 2.有一些private的参数作为对象的属性，然后针对每一个参数定义get和set方法访问的接口。
 * 3.没有从任何类继承、也没有实现任何接口，更没有被其它框架侵入的java对象
 */

public class student {
    private String id;
    private String name;
    private int age;
    private String phone;
    private String adress;

    //POJO类必须有无参构造函数
    public student() {

    }

    public student(String id, String name, int age, String phone, String adress) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.phone = phone;
        this.adress = adress;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getAdress() {
        return adress;
    }

    public void setAdress(String adress) {
        this.adress = adress;
    }


    @Override
    public String toString() {
        return "student{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", phone='" + phone + '\'' +
                ", adress='" + adress + '\'' +
                '}';
    }
}
