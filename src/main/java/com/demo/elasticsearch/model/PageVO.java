package com.demo.elasticsearch.model;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * @author 周泽
 * @date Create in 10:38 2018/11/29
 * @Description 分页结果集
 */
@Getter
@Setter
public class PageVO {
    /**
     * 当前页码
     */
    private Integer pageNum;

    /**
     * 一页显示数量
     */
    private Integer pageSize;

    /**
     * 总数量
     */
    private Integer total;

    /**
     * 结果集合
     */
    private List<Map<String,Object>> rList;

    /**
     * 共有多少页
     */
    private Integer pageCount;

    /**
     * 页码列表的开始索引(包含)
     */
    private Integer beginPageIndex;

    /**
     * 码列表的结束索引(包含)
     */
    private Integer endPageIndex;

    /**
     * 只接受前4个必要的属性，会自动的计算出其他3个属性的值
     * @param pageNum 当前页码
     * @param pageSize 每页显示条数
     * @param total 总条数
     * @param rList 结果集合
     */
    public PageVO(int pageNum, int pageSize, int total, List<Map<String, Object>> rList) {
        this.pageNum = pageNum;
        this.pageSize = pageSize;
        this.total = total;
        this.rList = rList;

        // 计算总页码
        pageCount = (total + pageSize - 1) / pageSize;

        // 计算 beginPageIndex 和 endPageIndex
        // >> 总页数不多于10页，则全部显示
        if (pageCount <= 10) {
            beginPageIndex = 1;
            endPageIndex = pageCount;
        } else {
            // >> 总页数多于10页，则显示当前页附近的共10个页码
            // 当前页附近的共10个页码（前4个 + 当前页 + 后5个）
            beginPageIndex = pageNum - 4;
            endPageIndex = pageNum + 5;

            // 当前面的页码不足4个时，则显示前10个页码
            if (beginPageIndex < 1) {
                beginPageIndex = 1;
                endPageIndex = 10;
            }

            // 当后面的页码不足5个时，则显示后10个页码
            if (endPageIndex > pageCount) {
                endPageIndex = pageCount;
                beginPageIndex = pageCount - 10 + 1;
            }
        }
    }

}
