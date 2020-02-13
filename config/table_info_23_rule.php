<?php
return [
    23 => [
        't_log_copy_kunlun' => [
            'comment' => 'copy_kunlun',
            'fields' => [
                'pid' => [
                    'type' => 'BIGINT',
                    'notes' => 'pid'
                ],
                'agent_id' => [
                    'type' => 'INT',
                    'notes' => '代理ID'
                ],
                'server_id' => [
                    'type' => 'INT',
                    'notes' => '区服ID'
                ],
                'upf' => [
                    'type' => 'INT',
                    'notes' => 'UPF'
                ],
                'role_id' => [
                    'type' => 'BIGINT',
                    'notes' => '角色ID'
                ],
                'regrow' => [
                    'type' => 'INT',
                    'notes' => '转生'
                ],
                'power' => [
                    'type' => 'INT',
                    'notes' => '战力'
                ],
                'soul_lv' => [
                    'type' => 'INT',
                    'notes' => '灵魂等级'
                ],
                'mtime' => [
                    'type' => 'INT',
                    'notes' => '时间戳'
                ]
            ]
        ],
        't_log_marry_star' => [
            'comment' => 'marry_star',
            'fields' => [
                'pid' => [
                    'type' => 'BIGINT',
                    'notes' => '代理ID'
                ],
                'agent_id' => [
                    'type' => 'INT',
                    'notes' => '代理ID'
                ],
                'server_id' => [
                    'type' => 'INT',
                    'notes' => '区服ID'
                ],
                'upf' => [
                    'type' => 'INT',
                    'notes' => 'UPF'
                ],
                'role_id' => [
                    'type' => 'BIGINT',
                    'notes' => '角色ID'
                ],
                'level' => [
                    'type' => 'INT',
                    'notes' => '角色等级'
                ],
                'is_internal' => [
                    'type' => 'INT',
                    'notes' => '是否内部号'
                ],
                'platform' => [
                    'type' => 'INT',
                    'notes' => '平台'
                ],
                'via' => [
                    'type' => 'INT',
                    'notes' => 'VIA' 
                ],
                'mtime' => [
                    'type' => 'INT',
                    'notes' => '时间戳'
                ],
                'op_type' => [
                    'type' => 'INT',
                    'notes' => '操作类型'
                ],
                'barter_id' => [
                    'type' => 'INT',
                    'notes' => '战斗ID'
                ],
                'barter_goods_id' => [
                    'type' => 'INT',
                    'notes' => '战斗商品ID'
                ],
                'request_id' => [
                    'type' => 'INT',
                    'notes' => '请求ID'
                ]
            ]
        ]
    ]
];